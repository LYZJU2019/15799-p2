use std::sync::Arc;
use std::time::{Instant, Duration};

use datafusion::arrow::array::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::{collect, ExecutionPlan}};
use anyhow::Result;
use async_recursion::async_recursion;
use itertools::Itertools;

use crate::sampling::{Plan, SampleOutput};

pub struct BenchmarkConfig {
	pub fast: bool,
	pub timeout: Option<Duration>,
}

// placeholder type
pub struct OptimizerMetrics;

/// Plan annotated with runtimes / cardinalities.
pub struct MeasuredPlan {
	pub plan: Plan,
	/// Time it took to run the overall plan. None if timeout hit.
	pub runtime: Option<Duration>,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<Option<usize>>,
	/// Preorder array of each subplan's runtime.
	// outer Option semantically means "we may not have ran this query"
	// inner Options semantically mean "we ran query and it timed out / died"
	pub sub_runtimes: Option<Vec<Option<Duration>>>,
}

pub struct BenchmarkOutput {
	/// Sorted by runtime (fastest at front).
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
}

async fn time_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	timeout: Option<Duration>,
) -> Result<Option<(Vec<RecordBatch>, Duration)>> {
	let (result_tx, result_rx) = tokio::sync::oneshot::channel();
	let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
	if let Some(timeout) = timeout {
		let (node, ctx) = (node.clone(), ctx.clone());
		let blocking_task = tokio::task::spawn(async move {
			let before = Instant::now();
			let out = collect(node, ctx).await;
			let after = Instant::now();
			let _ = result_tx.send((out, after-before));
		});
		tokio::spawn(async move {
			tokio::time::sleep(timeout).await;
			let _ = cancel_tx.send(());
		});
		tokio::select! {
			result = result_rx => {
				let (res, time) = result?;
				Ok(Some((res?, time)))
			}
			_ = cancel_rx => {
				Ok(None)
			}
		}
	} else {
		let (node, ctx) = (node.clone(), ctx.clone());
		let before = Instant::now();
		let out = collect(node, ctx).await;
		let after = Instant::now();
		Ok(Some((out?, after-before)))
	}
}

/// Recursively populate cardinality and runtime arrays.
// TODO need to be a *lot* more rigorous for the actual benchmarking here.
// one option is to try and integrate an existing optimizer like criterion
// or divan. Both of these don't really support usage as a library though...
// Doing this properly is an easy way to surpass TAQO.
//
// The other major TODO (this is long term) is to support the `fast` option 
// and implement the optimization in www.vldb.org/pvldb/vol2/vldb09-294.pdf
#[async_recursion]
async fn measure_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	cards: &mut Vec<Option<usize>>,
	times: &mut Vec<Option<Duration>>,
) -> Result<()> {
	if let Some((batches, time)) = time_subplan(node.clone(), ctx.clone(), cfg.timeout).await? {
		cards.push(Some(batches.iter().map(|x| x.num_rows()).sum()));
		times.push(Some(time));
	} else {
		cards.push(None);
		times.push(None);
	}
	for child in node.children() {
		measure_subplan(child.clone(), ctx.clone(), cfg, cards, times).await?;
	}
	Ok(())
}

/// Measure cardinalities and runtimes of plan and subplans.
async fn measure_plan(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
) -> Result<MeasuredPlan> {
	let mut cardinalities = Vec::new();
	let mut runtimes = Vec::new();
	// FIXME temporary hack to get around OOMs
	if plan.est_cost < 1000000000.0 {
		measure_subplan(plan.tree.clone(), ctx, cfg, &mut cardinalities, &mut runtimes).await?;
	} else {
		cardinalities.push(None);
		runtimes.push(None);
	}
	if let Some(runtime) = runtimes[0] {
		println!("ran plan in {}ms (est cost {})", runtime.as_millis(), plan.est_cost);
	} else {
		println!("plan timed out (est cost {})", plan.est_cost);
	}
	Ok(MeasuredPlan {
		plan,
		runtime: runtimes[0],
		cardinalities,
		sub_runtimes: Some(runtimes),
	})
}

pub async fn benchmark(sample: SampleOutput, cfg: BenchmarkConfig) -> Result<BenchmarkOutput> {
	let ctx = sample.session.task_ctx();
	let mut out = Vec::new();
	// TODO best measurement should definitely be interleaved in to avoid
	// warmup time affecting measurements or something like that...
	let best = measure_plan(sample.best_plan, ctx.clone(), &cfg).await?;
	for plan in sample.alternates.into_iter()
		.sorted_by(|x, y| x.est_cost.partial_cmp(&y.est_cost).unwrap()) {
		out.push(measure_plan(plan, ctx.clone(), &cfg).await?);
	}
	out.sort_by(|x, y| x.runtime.cmp(&y.runtime));
	let chosen_idx = out.iter()
		.position(|x| x.runtime > best.runtime).unwrap_or(out.len());
	out.insert(chosen_idx, best);
	
	// TODO actually measure metrics
	
	Ok(BenchmarkOutput {
		plans: out,
		chosen_idx,
		metrics: OptimizerMetrics
	})
}

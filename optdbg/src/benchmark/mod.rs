use std::sync::Arc;
use std::time::{Instant, Duration};

use datafusion::{execution::TaskContext, physical_plan::{collect, ExecutionPlan}};
use anyhow::Result;
use async_recursion::async_recursion;

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
	/// Time it took to run the overall plan.
	pub runtime: Duration,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<usize>,
	/// Preorder array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<Duration>>,
}

pub struct BenchmarkOutput {
	/// Sorted by runtime (fastest at front).
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
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
	cards: &mut Vec<usize>,
	times: &mut Vec<Duration>
) -> Result<()> {
	let future = collect(node.clone(), ctx.clone());
	let before = Instant::now();
	let batches = if let Some(timeout) = cfg.timeout {
		tokio::time::timeout(timeout, future).await??
	} else {
		future.await?
	};
	let after = Instant::now();
	cards.push(batches.iter().map(|x| x.num_rows()).sum());
	times.push(after-before);
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
	measure_subplan(plan.tree.clone(), ctx, cfg, &mut cardinalities, &mut runtimes).await?;
	println!("{}ms", runtimes[0].as_millis());
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
	for plan in sample.alternates {
		out.push(measure_plan(plan, ctx.clone(), &cfg).await?);
	}
	let best = measure_plan(sample.best_plan, ctx.clone(), &cfg).await?;
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

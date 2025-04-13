use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion_proto::bytes::physical_plan_to_bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use async_recursion::async_recursion;

use crate::sampling::SampleOutput;
use crate::common::{Plan, PlanMeasurements, MeasureError};

#[derive(Serialize, Deserialize)]
pub struct BenchmarkConfig {
	pub fast: bool,
	pub timeout: Option<Duration>,
}

// placeholder type
pub struct OptimizerMetrics;

pub struct BenchmarkOutput {
	/// Sorted by runtime (fastest at front).
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
}

/// Plan annotated with runtimes / cardinalities.
pub struct MeasuredPlan {
	pub plan: Plan,
	/// Time it took to run the overall plan. None if timeout hit.
	pub runtime: Result<Duration, MeasureError>,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<Result<usize, MeasureError>>,
	/// Preorder array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<Result<Duration, MeasureError>>>,
}

impl MeasuredPlan {
	fn new(
		plan: Plan,
		measurements: PlanMeasurements
	) -> Self {
		Self {
			plan,
			// FIXME need to change this when implementing covering queries!
			runtime: measurements.sub_runtimes.as_ref().unwrap()[0].clone(),
			cardinalities: measurements.cardinalities,
			sub_runtimes: measurements.sub_runtimes
		}
	}
}

/// Times a subplan's execution, giving up after a timeout.
async fn time_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	timeout: Option<Duration>,
) -> anyhow::Result<Option<(Vec<RecordBatch>, Duration)>> {
	let (result_tx, result_rx) = tokio::sync::oneshot::channel();
	let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
	if let Some(timeout) = timeout {
		let (node, ctx) = (node.clone(), ctx.clone());
		let _ = tokio::task::spawn(async move {
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
	cards: &mut Vec<Result<usize, MeasureError>>,
	times: &mut Vec<Result<Duration, MeasureError>>,
) -> anyhow::Result<()> {
	if let Some((batches, time)) = time_subplan(node.clone(), ctx.clone(), cfg.timeout).await? {
		cards.push(Ok(batches.iter().map(|x| x.num_rows()).sum()));
		times.push(Ok(time));
	} else {
		cards.push(Err(MeasureError::Timeout));
		times.push(Err(MeasureError::Timeout));
	}
	for child in node.children() {
		measure_subplan(child.clone(), ctx.clone(), cfg, cards, times).await?;
	}
	Ok(())
}

/// Measure cardinalities and runtimes of plan and subplans.
///
/// Use `measure_ipc` for OOM safety (which is a real concern).
// TODO think of a more consistent hack? do we bother keeping this at all?
async fn measure_plan(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
) -> anyhow::Result<MeasuredPlan> {
	let mut cardinalities = Vec::new();
	let mut runtimes = Vec::new();
	// FIXME temporary hack to get around OOMs. obviously not generic.
	if plan.est_costs[0] < 1000000000.0 {
		measure_subplan(plan.tree.clone(), ctx, cfg, &mut cardinalities, &mut runtimes).await?;
	} else {
		cardinalities.push(Err(MeasureError::Died));
		runtimes.push(Err(MeasureError::Died));
	}
	if let Ok(runtime) = runtimes[0] {
		println!("ran plan in {}ms (est cost {})", runtime.as_millis(), plan.est_costs[0]);
	} else {
		println!("plan timed out (est cost {})", plan.est_costs[0]);
	}
	Ok(MeasuredPlan {
		plan,
		runtime: runtimes[0],
		cardinalities,
		sub_runtimes: Some(runtimes),
	})
}

// this version is for cross-process stuff...
// but switching to `ListingTable`s kinda fixed OOM issues
/// Measure cardinalities and runtimes of plan and subplans.
async fn measure_plan_ipc(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<MeasuredPlan> {
	let bytes = physical_plan_to_bytes(plan.clone().tree)?;
	let mut plan_file = tempfile::NamedTempFile::new()?;
	plan_file.write_all(&bytes)?;

	let bytes = serde_json::to_string(cfg)?;
	let mut cfg_file = tempfile::NamedTempFile::new()?;
	cfg_file.write_all(bytes.as_bytes())?;

	let mut schemas = Vec::new();
	for i in tables.table_names() {
		schemas.push((i.clone(), tables.table(&i).await?.unwrap().schema()));
	}
	
	let bytes = serde_json::to_string(&schemas)?;
	let mut schema_file = tempfile::NamedTempFile::new()?;
	schema_file.write_all(bytes.as_bytes())?;

	let out_file = tempfile::NamedTempFile::new()?;
	// TODO need better solution than relative path lol
	let output = std::process::Command::new("../optdbg/target/release/runner")
		.arg("-p").arg(plan_file.path())
		.arg("-c").arg(cfg_file.path())
		.arg("-s").arg(schema_file.path())
		.arg("-o").arg(out_file.path())
		.status()?;
	
	if !output.success() {
		let size = plan.size();
		println!("died");
		Ok(MeasuredPlan {
			plan,
			runtime: Err(MeasureError::Died),
			cardinalities: vec![Err(MeasureError::Died); size],
			sub_runtimes: Some(vec![Err(MeasureError::Died); size]),
		})
	} else {
		let measurements: PlanMeasurements = serde_json::from_reader(out_file)?;
		Ok(MeasuredPlan::new(plan, measurements))
	}
}

pub async fn benchmark(
	sample: SampleOutput,
	cfg: BenchmarkConfig
) -> anyhow::Result<BenchmarkOutput> {
	let ctx = sample.session.task_ctx();
	let mut out = Vec::new();
	// TODO best measurement should definitely be interleaved in to avoid
	// warmup time affecting measurements or something like that...
	let best = measure_plan_ipc(sample.best_plan, ctx.clone(), &cfg, sample.tables.clone()).await?;
	for plan in sample.alternates.into_iter()
		.sorted_by(|x, y| x.est_costs[0].partial_cmp(&y.est_costs[0]).unwrap()) {
		out.push(measure_plan_ipc(plan, ctx.clone(), &cfg, sample.tables.clone()).await?);
	}
	out.sort_by(|x, y| {
		if x.runtime.is_err() {
			std::cmp::Ordering::Greater
		} else if y.runtime.is_err() {
			std::cmp::Ordering::Less
		} else {
			x.runtime.unwrap().cmp(&y.runtime.unwrap())
		}
	});
	let chosen_idx = out.iter()
		.position(|x| x.runtime.is_err() ||
				  best.runtime.is_ok() && x.runtime.unwrap() > best.runtime.unwrap())
		.unwrap_or(out.len());
	out.insert(chosen_idx, best);
	
	// TODO actually measure metrics
	Ok(BenchmarkOutput {
		plans: out,
		chosen_idx,
		metrics: OptimizerMetrics
	})
}

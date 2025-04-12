use std::env::temp_dir;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use anyhow::Result;
use datafusion_proto::bytes::physical_plan_to_bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::sampling::SampleOutput;
use crate::common::{Plan, PlanMeasurements};

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
	pub runtime: Option<Duration>,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<Option<usize>>,
	/// Preorder array of each subplan's runtime.
	// outer Option semantically means "we may not have ran this query"
	// inner Options semantically mean "we ran query and it timed out / died"
	pub sub_runtimes: Option<Vec<Option<Duration>>>,
}

impl MeasuredPlan {
	fn new(
		plan: Plan,
		measurements: PlanMeasurements
	) -> Self {
		Self {
			plan,
			runtime: measurements.sub_runtimes.as_ref().map(|x| x[0]).flatten(),
			cardinalities: measurements.cardinalities,
			sub_runtimes: measurements.sub_runtimes
		}
	}
}

/// Measure cardinalities and runtimes of plan and subplans.
async fn measure_plan(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	tables: &Vec<(String, Schema)>
) -> Result<MeasuredPlan> {
	println!("before");
	let bytes = physical_plan_to_bytes(plan.clone().tree)?;
	println!("after (please)");
	let mut plan_file = tempfile::NamedTempFile::new()?;
	plan_file.write_all(&bytes)?;

	let bytes = serde_json::to_string(cfg)?;
	let mut cfg_file = tempfile::NamedTempFile::new()?;
	cfg_file.write_all(&bytes.as_bytes())?;

	let bytes = serde_json::to_string(tables)?;
	let mut schema_file = tempfile::NamedTempFile::new()?;
	schema_file.write_all(&bytes.as_bytes())?;

	let out_file = tempfile::NamedTempFile::new()?;
	// TODO(quantumish) need better solution than relative path lol
	let output = std::process::Command::new("../optdbg/target/debug/runner")
		.arg("-p").arg(plan_file.path())
		.arg("-c").arg(cfg_file.path())
		.arg("-s").arg(schema_file.path())
		.arg("-o").arg(out_file.path())
		.output()?;

	let measurements: PlanMeasurements = serde_json::from_reader(out_file)?;
	
	// if let Some(runtime) = runtimes[0] {
	// 	println!("ran plan in {}ms (est cost {})", runtime.as_millis(), plan.est_cost);
	// } else {
	// 	println!("plan timed out (est cost {})", plan.est_cost);
	// }
	Ok(MeasuredPlan::new(plan, measurements))
}

pub async fn benchmark(sample: SampleOutput, cfg: BenchmarkConfig) -> Result<BenchmarkOutput> {
	let ctx = sample.session.task_ctx();
	let mut out = Vec::new();
	// TODO best measurement should definitely be interleaved in to avoid
	// warmup time affecting measurements or something like that...
	let best = measure_plan(sample.best_plan, ctx.clone(), &cfg, &sample.raw_tables).await?;
	for plan in sample.alternates.into_iter()
		.sorted_by(|x, y| x.est_cost.partial_cmp(&y.est_cost).unwrap()) {
		out.push(measure_plan(plan, ctx.clone(), &cfg, &sample.raw_tables).await?);
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

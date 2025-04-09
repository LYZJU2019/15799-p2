use datafusion::physical_plan::collect;
use anyhow::Result;

use crate::sampling::{Plan, SampleOutput};

// placeholder type
pub struct OptimizerMetrics {
	pub optimality: f32,
	pub efficiency: f32,
}

pub struct MeasuredPlan {
	pub plan: Plan,
	/// Time it took to run the plan.
	pub runtime: chrono::Duration,
	/// Topologically sorted array of each subplan's true cardinality.
	pub cardinalities: Vec<usize>,
	/// Topologically sorted array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<chrono::Duration>>,
}

pub struct BenchmarkOutput {
	/// Sorted by runtime.
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
}

pub async fn benchmark(
	sample: SampleOutput
) -> Result<BenchmarkOutput> {
	let _batches = collect(sample.best_plan.tree, sample.session.task_ctx()).await?;
	todo!()
}

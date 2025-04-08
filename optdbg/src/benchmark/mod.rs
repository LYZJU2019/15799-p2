use std::sync::Arc;

use datafusion::physical_plan::{collect, ExecutionPlan};
use anyhow::Result;

use crate::sampling::SampleOutput;

// placeholder type
pub struct OptimizerMetrics {
	pub optimality: f32,
	pub efficiency: f32,
}

pub struct BenchmarkOutput {
	pub notable_plans: Vec<(Arc<dyn ExecutionPlan>, Vec<usize>)>,
	pub metrics: OptimizerMetrics,
}

pub async fn benchmark(
	sample: SampleOutput
) -> Result<BenchmarkOutput> {
	let _batches = collect(sample.best_plan, sample.session.task_ctx()).await?;
	todo!()
}

use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

// placeholder type
pub struct OptimizerMetrics {
	pub _optimality: f32,
	pub _efficiency: f32,
}

pub fn benchmark(
	_plans: Vec<Arc<dyn ExecutionPlan>>
) -> (Vec<(Arc<dyn ExecutionPlan>, Vec<usize>)>, OptimizerMetrics) {
	todo!()
}

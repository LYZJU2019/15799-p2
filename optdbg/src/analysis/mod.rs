use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

use crate::benchmark::OptimizerMetrics;

// Placeholder type.
pub struct Report;

impl std::fmt::Display for Report {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

pub fn analyze(
	_card_plans: Vec<(Arc<dyn ExecutionPlan>, Vec<usize>)>,
	_metrics: OptimizerMetrics
) -> Report {
	todo!()
}

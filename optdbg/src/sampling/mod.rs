use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

pub fn sample(_query: String) -> Vec<Arc<dyn ExecutionPlan>> {
	todo!()
}

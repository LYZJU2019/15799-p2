use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

use crate::benchmark::{BenchmarkOutput, MeasuredPlan};

// Placeholder type.
pub struct Report;

impl std::fmt::Display for Report {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

pub fn compare_plans(a: &MeasuredPlan, b: &MeasuredPlan) -> () {
	todo!()
}

pub fn analyze(bench: BenchmarkOutput) -> Report {
	let chosen = &bench.plans[bench.chosen_idx];
	for plan in &bench.plans[bench.chosen_idx..] {
		compare_plans(chosen, plan);
	}
	Report
}

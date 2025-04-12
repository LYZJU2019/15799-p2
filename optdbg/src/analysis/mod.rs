use crate::benchmark::{BenchmarkOutput, MeasuredPlan};

// Placeholder type.
pub struct AnalysisConfig;	

// Placeholder type.
pub struct Report;

impl std::fmt::Display for Report {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

pub fn compare_plans(_a: &MeasuredPlan, _b: &MeasuredPlan) -> () {
	todo!()
}

pub fn analyze(bench: BenchmarkOutput, _cfg: AnalysisConfig) -> Report {
	let chosen = &bench.plans[bench.chosen_idx];
	for plan in &bench.plans[..bench.chosen_idx] {
		println!("optimal plan not chosen!");
		compare_plans(chosen, plan);
	}
	Report
}

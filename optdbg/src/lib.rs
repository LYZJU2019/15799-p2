pub mod sampling;
pub mod benchmark;
pub mod analysis;

pub fn report_query(query: String) -> analysis::Report {
	let plans = sampling::sample(query);
	let (notable_plans, metrics) = benchmark::benchmark(plans);
	analysis::analyze(notable_plans, metrics)
}

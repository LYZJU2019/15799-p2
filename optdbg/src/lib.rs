use anyhow::Result;
use sampling::QueryInfo;

pub mod common;
pub mod sampling;
pub mod benchmark;
pub mod analysis;

pub async fn report_query(
	queries: impl futures::Stream<Item = QueryInfo>,
	s_cfg: sampling::SampleConfig,
	b_cfg: benchmark::BenchmarkConfig,
	a_cfg: analysis::AnalysisConfig
) -> Result<Vec<analysis::Report>> {
	let plans = sampling::sample(queries, s_cfg);	
	let bench = benchmark::benchmark(plans, b_cfg);
	Ok(analysis::analyze(bench, a_cfg).await)
}

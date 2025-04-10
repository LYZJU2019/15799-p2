use anyhow::Result;

pub mod sampling;
pub mod benchmark;
pub mod analysis;

pub async fn report_query(
	query: String,
	s_cfg: sampling::SampleConfig,
	b_cfg: benchmark::BenchmarkConfig,
	a_cfg: analysis::AnalysisConfig
) -> Result<analysis::Report> {
	let plans = sampling::sample(query, s_cfg).await?;
	let bench = benchmark::benchmark(plans, b_cfg).await?;
	Ok(analysis::analyze(bench, a_cfg))
}

use anyhow::Result;

pub mod sampling;
pub mod benchmark;
pub mod analysis;

pub async fn report_query(
	query: String,
	opt: sampling::OptimizerBackend
) -> Result<analysis::Report> {
	let plans = sampling::sample(query, opt).await?;
	let (notable_plans, metrics) = benchmark::benchmark(plans);
	Ok(analysis::analyze(notable_plans, metrics))
}

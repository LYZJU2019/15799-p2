use std::path::Path;

use clap::Parser;

mod sampling;
mod benchmark;
mod analysis;

use sampling::OptimizerBackend;

/// Query optimizer debugger
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the query to optimize
    #[arg(short, long)]
    query_path: String,

	/// Optimizer to evaluate
	#[arg(short, long)]
    optimizer: OptimizerBackend,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
	let query = std::fs::read_to_string(
		Path::new(&args.query_path)
	).expect("read query from file");
	let plans = sampling::sample(query, OptimizerBackend::Optd).await?;
	let bench = benchmark::benchmark(plans).await?;
	let report = analysis::analyze(bench.notable_plans, bench.metrics);
	println!("{report}");
	Ok(())
}

use std::path::Path;
use std::time::Duration;

use clap::Parser;

mod sampling;
mod benchmark;
mod analysis;

use sampling::{SampleConfig, OptimizerBackend};
use benchmark::BenchmarkConfig;
use analysis::AnalysisConfig;

impl std::str::FromStr for Duration {
	type Err = &'static str;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let num_portion = s.chars().take_while(|x| *x.is_numeric()).collect();
		let num = usize::from_str(&num_portion).map_err(|_| "bad number")?;
		match &s.chars().filter(|x| !*x.is_numeric()).collect() {
			"s" => Ok(Duration::from_secs(num)),
			"m" => Ok(Duration::from_secs(num * 60)),
			"h" => Ok(Duration::from_secs(num * 60 * 60)),
			_ => Err("bad suffix")
		}
	}
}

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

	/// Avoid running all subplans of a plan
	#[arg(short, long)]
	fast: bool,

	/// Timeout to use for benchmarking ([0-9]+(m|s|h))
	#[arg(short, long)]
	timeout: Option<Duration>	
}

impl Args {
	fn to_parts(self) -> (SampleConfig, BenchmarkConfig, AnalysisConfig) {
		(
			SampleConfig {
				backend: self.optimizer
			},
			BenchmarkConfig {
				timeout: self.timeout,
				fast: self.fast,				
			},
			AnalysisConfig
		)
	}
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (s_cfg, b_cfg, a_cfg) = Args::parse().to_parts();
	let query = std::fs::read_to_string(
		Path::new(&args.query_path)
	).expect("read query from file");
	let plans = sampling::sample(query, s_cfg).await?;
	let bench = benchmark::benchmark(plans, b_cfg).await?;
	let report = analysis::analyze(bench, a_cfg);
	println!("{report}");
	Ok(())
}

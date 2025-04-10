use std::path::Path;
use std::time::Duration;

use clap::Parser;

mod sampling;
mod benchmark;
mod analysis;

use sampling::{OptimizerBackend, QueryInfo, SampleConfig};
use benchmark::BenchmarkConfig;
use analysis::AnalysisConfig;

// Get around inability to implement foreign trait for foreign type.
#[derive(Clone, Debug)]
struct TimeoutTime(Duration);

impl std::str::FromStr for TimeoutTime {
	type Err = &'static str;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let num_portion: String = s.chars().take_while(|x| x.is_numeric()).collect();
		let num = u64::from_str(&num_portion).map_err(|_| "bad number")?;
		match s.chars().filter(|x| !x.is_numeric()).collect::<String>().as_str() {
			"s" => Ok(TimeoutTime(Duration::from_secs(num))),
			"m" => Ok(TimeoutTime(Duration::from_secs(num * 60))),
			"h" => Ok(TimeoutTime(Duration::from_secs(num * 60 * 60))),
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
	timeout: Option<TimeoutTime>	
}

impl Args {
	fn to_configs(self) -> (SampleConfig, BenchmarkConfig, AnalysisConfig) {
		(
			SampleConfig {
				backend: self.optimizer
			},
			BenchmarkConfig {
				timeout: self.timeout.map(|x| x.0),
				fast: self.fast,
			},
			AnalysisConfig
		)
	}
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
	let query = std::fs::read_to_string(
		Path::new(&args.query_path)
	).expect("read query from file");
	let (s_cfg, b_cfg, a_cfg) = args.to_configs();
	let query = QueryInfo {
		query,
		tables: todo!("figure out interface for accessing db")
	};
	let plans = sampling::sample(query, s_cfg).await?;
	let bench = benchmark::benchmark(plans, b_cfg).await?;
	let report = analysis::analyze(bench, a_cfg);
	println!("{report}");
	Ok(())
}

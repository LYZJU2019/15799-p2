mod lib;
use std::time::Duration;

use lib::run_benchmark_with_config;
use optdbg::benchmark::BenchmarkConfig;
use optdbg::analysis::ReportedProblem;
use optdbg::analysis::MisestimationKind;

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Whether or not to use the advanced cost model for optd
    #[arg(short, long)]
    adv_cost: bool,

	/// Whether or not to use the root-only problem finding mode
    #[arg(short, long)]
    root_only: bool,

	/// The node / predicate we expect to have an error reported for.
	#[arg(short, long)]
	expect_err: String,

	/// The kind of problem we're looking for.
	#[arg(short, long)]
	expect_kind: String,
	
	/// Do we expect over / under estimation or just any error? 
	#[arg(short, long)]
	expect_by: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();
    // Get data directory from environment variable or use default
    let data_dir = std::env::var("OPTDBG_DATA_DIR").unwrap_or_else(|_| "./tpch-data".to_string());
    // Get queries directory from environment variable or use default
    let queries_dir = std::env::var("OPTDBG_QUERIES_DIR").unwrap_or_else(|_| "./tpch-queries".to_string());
    
    println!("Using data directory: {}", data_dir);
    println!("Using queries directory: {}", queries_dir);
    
    // Set environment variable for runner command
    std::env::set_var("OPTDBG_RUNNER_PATH", "../optdbg/target/release/runner");
    
	let config1 = BenchmarkConfig {
        timeout: Some(Duration::from_secs(3)),
        fast: false,
        enable_cache: true,
        num_runs: 5,
        drop_outliers: false,
        overlap_threshold: 0.5,
        early_stopping: true,
        track_metrics: true,
    };

	let report = run_benchmark_with_config(config1, &data_dir, &queries_dir,
										   args.adv_cost, args.root_only).await?;

	println!("{report}");

	let mut qualified = false;
	for (n, p) in report.report_problems() {
		match p {
			ReportedProblem::Cardinality(k) => {
				if args.expect_err == n &&
					args.expect_kind == "cardinality" &&
					args.expect_by == k.to_string()
				{
					qualified = true;
				}
			}
			ReportedProblem::Cost(k) => {
				if args.expect_err == n &&
					args.expect_kind == "cost" &&
					args.expect_by == k.to_string()
				{
					qualified = true;
				}
			}
		}
	}
	if !qualified {
		panic!("Test failed: got problems {:?}", report.report_problems());
	}
			
	Ok(())
}

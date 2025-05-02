mod lib;
use std::time::Duration;

use lib::run_benchmark_with_config;
use optdbg::benchmark::BenchmarkConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

	run_benchmark_with_config(config1, &data_dir, &queries_dir).await?;
	Ok(())
}

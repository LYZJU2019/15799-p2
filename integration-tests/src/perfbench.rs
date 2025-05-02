use std::time::{Duration, Instant};

mod lib;
use lib::run_benchmark_with_config;
use optdbg::benchmark::{BenchmarkConfig, reset_performance_metrics, report_performance_metrics};

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Whether or not to use the advanced cost model for optd
    #[arg(short, long)]
    adv_cost: bool,
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
    
    // Define different benchmark configurations
    println!("\n=== PERFORMANCE COMPARISON ===\n");
    
    // Test 1: With caching and early stopping (default)
    println!("\n=== Configuration 1: With Caching and Early Stopping ===\n");
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
    
    let start1 = Instant::now();
    reset_performance_metrics();
    let report = run_benchmark_with_config(config1, &data_dir,
										   &queries_dir, args.adv_cost, true).await?;
    let duration1 = start1.elapsed();
	println!("{report}");
    println!("{}", report_performance_metrics());
    println!("Configuration 1 total time: {:.2}s", duration1.as_secs_f64());
    
    // Test 2: Without caching (to measure cache impact)
    println!("\n=== Configuration 2: Without Caching ===\n");
    let config2 = BenchmarkConfig {
        timeout: Some(Duration::from_secs(3)),
        fast: false,
        enable_cache: false,
        num_runs: 5,
        drop_outliers: false,
        overlap_threshold: 0.5,
        early_stopping: true,
        track_metrics: true,
    };
    
    let start2 = Instant::now();
    reset_performance_metrics();
    let report = run_benchmark_with_config(config2, &data_dir,
										   &queries_dir, args.adv_cost, true).await?;
    let duration2 = start2.elapsed();
	println!("{report}");
    println!("{}", report_performance_metrics());
    println!("Configuration 2 total time: {:.2}s", duration2.as_secs_f64());
    
    // Test 3: Without early stopping (to measure early stopping impact)
    println!("\n=== Configuration 3: Without Early Stopping ===\n");
    let config3 = BenchmarkConfig {
        timeout: Some(Duration::from_secs(3)),
        fast: false,
        enable_cache: true,
        num_runs: 5,
        drop_outliers: false,
        overlap_threshold: 0.5,
        early_stopping: false,
        track_metrics: true,
    };
    
    let start3 = Instant::now();
    reset_performance_metrics();
    let report = run_benchmark_with_config(config3, &data_dir,
										   &queries_dir, args.adv_cost, true).await?;
    let duration3 = start3.elapsed();
	println!("{report}");
    println!("{}", report_performance_metrics());
    println!("Configuration 3 total time: {:.2}s", duration3.as_secs_f64());
    
    // Performance summary
    println!("\n=== PERFORMANCE SUMMARY ===\n");
    println!("With caching and early stopping: {:.2}s", duration1.as_secs_f64());
    println!("Without caching: {:.2}s", duration2.as_secs_f64());
    println!("Without early stopping: {:.2}s", duration3.as_secs_f64());
    
    // Calculate performance improvements
    if duration2.as_secs_f64() > 0.0 {
        let caching_improvement = ((duration2.as_secs_f64() - duration1.as_secs_f64()) / duration2.as_secs_f64()) * 100.0;
        println!("Caching improved performance by {:.2}%", caching_improvement);
    }
    
    if duration3.as_secs_f64() > 0.0 {
        let early_stopping_improvement = ((duration3.as_secs_f64() - duration1.as_secs_f64()) / duration3.as_secs_f64()) * 100.0;
        println!("Early stopping improved performance by {:.2}%", early_stopping_improvement);
    }

    Ok(())
}

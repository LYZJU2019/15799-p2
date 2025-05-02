use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::prelude::ParquetReadOptions;
use datafusion_expr::LogicalPlan;
use optdbg::sampling::{OptdOldBackend, RuleBailStrategy, SampleStrategy};
use optdbg::{
    analysis::AnalysisConfig,
    benchmark::{BenchmarkConfig, reset_performance_metrics, report_performance_metrics},
    sampling::{QueryInfo, SampleConfig},
};
use test_utils::tpch::tpch_schemas;

async fn run_benchmark_with_config(config: BenchmarkConfig, data_dir: &str, queries_dir: &str) -> anyhow::Result<()> {
    let s_cfg = SampleConfig;
    let a_cfg = AnalysisConfig {
        root_problems_only: true,
    };

    let df_ctx = SessionContext::new_with_config(SessionConfig::default());
    let schemas = tpch_schemas();
    let mut table_paths = Vec::new();
    for tableref in &schemas {
        let options = ParquetReadOptions::new().schema(&tableref.schema);
        let path = format!("{}/{}.parquet", data_dir, tableref.name);
        let table_path = std::path::Path::new(&path).canonicalize()?;
        table_paths.push((tableref.name.clone(), table_path.clone()));
        df_ctx
            .register_parquet(tableref.name.clone(), table_path.to_str().unwrap(), options)
            .await?;
    }

    let paths = std::fs::read_dir(queries_dir).unwrap();
    let stream = futures::stream::iter(paths.into_iter()).then(|path| {
        let df_ctx = df_ctx.clone();
        let table_paths = table_paths.clone();
        async move {
            let path_result = path.map_err(|e| anyhow::anyhow!("Failed to read directory entry: {}", e));
            let path_entry = match path_result {
                Ok(entry) => entry,
                Err(e) => return Err(e),
            };
            
            let file_path = path_entry.path();
            println!("{}", file_path.display());
            
            // Read as bytes first, then handle UTF-8 conversion
            let sql_bytes = match std::fs::read(&file_path) {
                Ok(bytes) => bytes,
                Err(e) => return Err(anyhow::anyhow!("Failed to read file {:?}: {}", file_path, e)),
            };
            
            // Try to convert to UTF-8 string
            let sql = match String::from_utf8(sql_bytes) {
                Ok(s) => s,
                Err(e) => return Err(anyhow::anyhow!("File {:?} contains invalid UTF-8: {}", file_path, e)),
            };
            
            let df = df_ctx.sql(&sql).await?;
            let (state, plan) = df.into_parts();
            let tables = df_ctx.state().schema_for_ref("part")?;
            
            // Convert path to string, handling potential UTF-8 errors
            let name = match file_path.into_os_string().into_string() {
                Ok(s) => s,
                Err(os_str) => format!("<non-utf8-path-{:?}>", os_str),
            };
            
            Ok(QueryInfo {
                name,
                plan,
                backend: Arc::new(
                    OptdOldBackend::new(
                        tables.clone(),
                        table_paths,
                        SampleStrategy::RuleBased(RuleBailStrategy::Never),
                        false,
                    )
                        .await?,
                ),
                state,
                tables,
            })
        }
    }).filter_map(|x: anyhow::Result<QueryInfo>| async {
        if let Err(ref e) = x {
            println!("{e}");
        }
        x.ok()
    });
    
    let report = optdbg::report_query(stream, s_cfg, config, a_cfg).await?;
    println!("{report}");
    
    // Report final performance metrics
    println!("{}", report_performance_metrics());

    Ok(())
}

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
    run_benchmark_with_config(config1, &data_dir, &queries_dir).await?;
    let duration1 = start1.elapsed();
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
    run_benchmark_with_config(config2, &data_dir, &queries_dir).await?;
    let duration2 = start2.elapsed();
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
    run_benchmark_with_config(config3, &data_dir, &queries_dir).await?;
    let duration3 = start3.elapsed();
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

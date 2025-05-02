use std::io::Write;
use std::sync::Arc;

use futures::StreamExt;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::prelude::ParquetReadOptions;
use datafusion_expr::LogicalPlan;
use optdbg::sampling::{OptdOldBackend, RuleBailStrategy, SampleStrategy};
use optdbg::{
    analysis::AnalysisConfig,
    benchmark::BenchmarkConfig,
    sampling::{QueryInfo, SampleConfig},
};
use test_utils::tpch::tpch_schemas;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let s_cfg = SampleConfig;

    let b_cfg = BenchmarkConfig {
        // Set timeout with enhanced timeout mechanism:
        // 1. Execute from top to bottom, skip all child nodes if parent node times out
        // 2. Added panic catching to prevent Arrow library errors from crashing the program
        // 3. Added hard timeout to ensure the task will terminate
        timeout: Some(std::time::Duration::from_secs(1)),
        fast: false,
        enable_cache: true, // Enable plan caching to avoid re-running identical plans
        num_runs: 5, // Run each plan 5 times to get statistical significance
        drop_outliers: true, // Drop highest and lowest measurements to reduce noise
        overlap_threshold: 0.5, // Consider runtimes equal if their ranges overlap by 50%
		early_stopping: true, // Skip measuring subplans of failing plans
    };

    let a_cfg = AnalysisConfig;

    let config = SessionConfig::default();
    let df_ctx = SessionContext::new_with_config(config);
    let schemas = tpch_schemas();
    let mut table_paths = Vec::new();
    for tableref in &schemas {
        let options = ParquetReadOptions::new().schema(&tableref.schema);
        let path = format!("./tpch-data/{}.parquet", tableref.name);
        let table_path = std::path::Path::new(&path).canonicalize()?;
        table_paths.push((tableref.name.clone(), table_path.clone()));
        df_ctx
            .register_parquet(tableref.name.clone(), table_path.to_str().unwrap(), options)
            .await?;
    }

	let paths = std::fs::read_dir("./tpch-queries").unwrap();
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
						true,
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
	
	let report = optdbg::report_query(stream, s_cfg, b_cfg, a_cfg).await?;
	println!("{report}");

    Ok(())
}

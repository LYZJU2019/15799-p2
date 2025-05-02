use std::sync::Arc;
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

pub async fn run_benchmark_with_config(config: BenchmarkConfig, data_dir: &str, queries_dir: &str) -> anyhow::Result<()> {
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

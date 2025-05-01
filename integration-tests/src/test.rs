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
        timeout: Some(std::time::Duration::from_secs(2)),
        fast: false,
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
			let sql = std::fs::read_to_string(path.as_ref().unwrap().path()).unwrap();
			let df = df_ctx.sql(&sql).await?;
			let (state, plan) = df.into_parts();
			let tables = df_ctx.state().schema_for_ref("part")?;
			Ok(QueryInfo {
				name: path.unwrap().path().into_os_string().into_string().unwrap(),
				plan,
				backend: Arc::new(
					OptdOldBackend::new(
						tables.clone(),
						table_paths,
						SampleStrategy::RuleBased(RuleBailStrategy::Threshold(10)),
						true,
					)
						.await?,
				),
				state,
				tables,
			})
		}
	}).filter_map(|x: anyhow::Result<QueryInfo>| async { x.ok() });
	
	let reports = optdbg::report_query(stream, s_cfg, b_cfg, a_cfg).await?;
	for report in reports {
		println!("{report}");
	}

    Ok(())
}

fn dump_plan(plan: &LogicalPlan, name: &str) {
    let mut file = std::fs::File::create(name).unwrap();

    file.write(format!("{:#?}", plan).as_bytes()).unwrap();

    file.flush().unwrap();
}

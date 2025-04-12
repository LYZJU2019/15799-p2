use std::sync::Arc;

use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::MemTable;
use datafusion::{
	datasource::{listing::PartitionedFile, physical_plan::{CsvSource, FileSource, FileStream}},
	execution::context::{SessionConfig, SessionContext}, physical_plan::metrics::ExecutionPlanMetricsSet
};
use datafusion_execution::object_store::ObjectStoreUrl;
use optdbg::sampling::{OptdOldBackend, SampleStrategy};
use optdbg::{
	analysis::AnalysisConfig, benchmark::BenchmarkConfig,
	sampling::{SampleConfig, QueryInfo}
};
use test_utils::tpch::tpch_schemas;
use futures::StreamExt;
use object_store::{ObjectStore, local::LocalFileSystem};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	// tracing_subscriber::fmt()
	// 	.with_max_level(tracing::Level::DEBUG)
	// 	.init();
	
	let tpch_query_9 = "
select
	nation,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			l_extendedprice - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%:1%'
	) as profit
group by
	nation
LIMIT 1;
";

	let s_cfg = SampleConfig;

	let b_cfg = BenchmarkConfig {
		timeout: Some(std::time::Duration::from_millis(50)),
		fast: false,
	};

	let a_cfg = AnalysisConfig;

	let mut tables = Vec::new();
	for tableref in tpch_schemas() {
		let schemaref = Arc::new(tableref.schema);
		let object_store = Arc::new(LocalFileSystem::new());
		println!("loading {}", tableref.name);
		let path = format!("./tpch-data/{}.tbl", tableref.name);
		let path = std::path::Path::new(&path).canonicalize()?;
		let scan_config = FileScanConfig::new(
			ObjectStoreUrl::local_filesystem(),
			schemaref.clone(),
			Arc::new(CsvSource::default())
		).with_file(PartitionedFile::new(
			path.display().to_string(), 10
		));
		let config = CsvSource::new(true, b'|', b'"')
			.with_batch_size(8192)
			.with_schema(schemaref.clone());
		let opener = config
			.create_file_opener(object_store, &scan_config, 0);
		let mut result = vec![];
		let mut stream =
			FileStream::new(&scan_config, 0, opener, &ExecutionPlanMetricsSet::new())?;
		while let Some(batch) = stream.next().await.transpose()? {
			result.push(batch);
		}
		tables.push((
			tableref.name,
			Arc::new(MemTable::try_new(schemaref.clone(), vec![result])?)
		));
	}	

	let config = SessionConfig::default();
	let df_ctx = SessionContext::new_with_config(config);
	for (name, table) in &tables {
		df_ctx.register_table(name, table.clone())?;
	}
	let df = df_ctx.sql(tpch_query_9).await?;
	let (state, plan) = df.into_parts();

	let query = QueryInfo {
		plan,
		backend: Arc::new(OptdOldBackend::new(
			&tables, SampleStrategy::RuleBased(Some(8))
		).await?),
		state,
		tables,
	};
	
	optdbg::report_query(query, s_cfg, b_cfg, a_cfg).await?;
	
    Ok(())
}

use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::{
	datasource::{listing::PartitionedFile, physical_plan::{CsvSource, FileScanConfigBuilder, FileSource, FileStream}},
	execution::context::{SessionConfig, SessionContext}, physical_plan::metrics::ExecutionPlanMetricsSet
};
use datafusion_execution::object_store::ObjectStoreUrl;
use optdbg::{
	analysis::AnalysisConfig, benchmark::BenchmarkConfig,
	sampling::{OptimizerBackend, SampleConfig, QueryInfo}
};
use test_utils::tpch::tpch_schemas;
use futures::StreamExt;
use object_store::{ObjectStore, local::LocalFileSystem};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	env_logger::init();
	let tpch_query_9 = "
select
	l_returnflag,
	l_linestatus
from
	lineitem
where
	l_shipdate <= date '1998-12-01';
";

	let s_cfg = SampleConfig {
		backend: OptimizerBackend::OptdOld,		
	};

	let b_cfg = BenchmarkConfig {
		timeout: None,
		fast: false,
	};

	let a_cfg = AnalysisConfig;

	let mut tables = Vec::new();
	for tableref in tpch_schemas() {
		let schemaref = Arc::new(tableref.schema);
		let object_store = Arc::new(LocalFileSystem::new());
		println!("reading {}", tableref.name);
		let path = format!("./tpch-data/{}.tbl", tableref.name);
		let path = std::path::Path::new(&path).canonicalize()?;
		let scan_config = FileScanConfigBuilder::new(
			ObjectStoreUrl::local_filesystem(),
			schemaref.clone(),
			Arc::new(CsvSource::default())
		).with_file(PartitionedFile::new(
			path.display().to_string(), 10
		)).build();
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

	let query = QueryInfo {
		query: tpch_query_9.to_string(),
		tables: unsafe { std::mem::transmute(tables) },
	};
	
	optdbg::report_query(query, s_cfg, b_cfg, a_cfg).await?;
	
    Ok(())
}

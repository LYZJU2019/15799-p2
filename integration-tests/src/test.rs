use std::sync::Arc;

use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::options::ReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion_catalog::{MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion_common::TableReference;
use optdbg::sampling::{OptdOldBackend, SampleStrategy, RuleBailStrategy};
use optdbg::{
	analysis::AnalysisConfig, benchmark::BenchmarkConfig,
	sampling::{SampleConfig, QueryInfo}
};
use test_utils::tpch::tpch_schemas;

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
		timeout: Some(std::time::Duration::from_secs(1)),
		fast: false,
	};

	let a_cfg = AnalysisConfig;

	let config = SessionConfig::default();
	let df_ctx = SessionContext::new_with_config(config);
	let schemas = tpch_schemas();
	for tableref in &schemas {
		let options = CsvReadOptions::new().delimiter(b'|').quote(b'"')
			.schema(&tableref.schema);
		let path = format!("./tpch-data/{}.csv", tableref.name);
		let table_path = std::path::Path::new(&path).canonicalize()?;
		df_ctx.register_csv(tableref.name.clone(), table_path.to_str().unwrap(), options).await?;
	}	

	let df = df_ctx.sql(tpch_query_9).await?;
	
	let (state, plan) = df.into_parts();

	let tables = df_ctx.state().schema_for_ref("part")?;	
	let query = QueryInfo {
		plan,
		backend: Arc::new(OptdOldBackend::new(
			tables.clone(),
			SampleStrategy::RuleBased(RuleBailStrategy::Threshold(8))
		).await?),
		state,
		tables,
	};
	
	optdbg::report_query(query, s_cfg, b_cfg, a_cfg).await?;
	
    Ok(())
}

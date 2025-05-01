use std::io::Write;
use std::sync::Arc;

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

    let ex_tpch = "
SELECT 1 FROM Lineitem, Orders, Customer
WHERE l_orderkey = o_orderkey
AND o_custkey = c_custkey
AND l_shipdate > date '2008-01-01'
AND l_receiptdate < date '2008-02-01'
AND l_discount < 0.05
AND o_orderpriority = 'HIGH'
AND c_mktsegment = 'AUTOMOBILE'
";

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

    let df = df_ctx.sql(&tpch_query_9).await?;

    let (state, plan) = df.into_parts();

    dump_plan(&plan, "original_plan.txt");

    let tables = df_ctx.state().schema_for_ref("part")?;
    let query = QueryInfo {
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
    };

    optdbg::report_query(query, s_cfg, b_cfg, a_cfg).await?;

    Ok(())
}

fn dump_plan(plan: &LogicalPlan, name: &str) {
    let mut file = std::fs::File::create(name).unwrap();

    file.write(format!("{:#?}", plan).as_bytes()).unwrap();

    file.flush().unwrap();
}

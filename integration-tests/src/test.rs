use std::sync::Arc;

use datafusion_common::Result;
use datafusion_catalog::memory::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use test_utils::tpch::tpch_schemas;

#[tokio::main]
async fn main() -> Result<()> {
    let tpch_query_9 = "
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
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
	nation,
	o_year
order by
	nation,
	o_year desc
LIMIT 1;
";

    let config = SessionConfig::default();
    let ctx = SessionContext::new_with_config(config);

    let tables = tpch_schemas();
    for table in tables {
        ctx.register_table(
            table.name,
            Arc::new(MemTable::try_new(Arc::new(table.schema.clone()), vec![])?),
        )?;
    }
    
	let df = ctx.sql(tpch_query_9).await?;
	let (st, pl) = df.into_parts();
	let pl = st.optimize(&pl)?;
	let phys = st.create_physical_plan(&pl).await?;
	
	optdbg::benchmark::benchmark(vec![unsafe { std::mem::transmute(phys) }]);
	
    Ok(())
}

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::{SessionConfig, SessionContext};
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use anyhow::Result;
use optd_og_datafusion_bridge::OptdPlanContext;
use optd_og_core::cascades::Memo;
use optd_og_datafusion_repr::cost::COMPUTE_COST;

#[derive(Clone, Debug)]
pub enum OptimizerBackend {
	Optd,
	OptdOld,
	Dolomite,
}

pub struct SampleConfig {
	pub backend: OptimizerBackend
}

impl std::str::FromStr for OptimizerBackend {
	type Err = &'static str;
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"optd" => Ok(OptimizerBackend::Optd),
			"optd-old" => Ok(OptimizerBackend::OptdOld),
			"dolomite" => Ok(OptimizerBackend::Dolomite),
			_ => Err("unknown backend")
		}
	}
}

pub struct Plan {
	pub tree: Arc<dyn ExecutionPlan>,
	pub est_cost: f64,
}

impl Plan {
	fn new(tree: Arc<dyn ExecutionPlan>, est_cost: f64) -> Self {
		Self { tree, est_cost } 
	}
}

pub struct SampleOutput {
	pub best_plan: Plan,
	pub alternates: Vec<Plan>,
	pub session: SessionState,
}

pub struct QueryInfo {
	pub query: String,
	pub tables: Vec<(String, Arc<MemTable>)>,
}

pub async fn sample(query: QueryInfo, cfg: SampleConfig) -> Result<SampleOutput> {
	let config = SessionConfig::default();
	let df_ctx = SessionContext::new_with_config(config);
	for (name, table) in &query.tables {
		df_ctx.register_table(name, table.clone())?;
	}
	let df = df_ctx.sql(&query.query).await?;
	let (st, pl) = df.into_parts();
	match cfg.backend {
		OptimizerBackend::Optd => {
			use optd_datafusion::df_conversion::context::OptdDFContext;
			let mut opt_ctx = OptdDFContext::new(&st);
			let _plan = opt_ctx.df_to_optd_relational(&pl);
			todo!("There isn't really a way to run new optd yet.")
		},
		OptimizerBackend::OptdOld => {			
			let mut opt_ctx = OptdPlanContext::new(&st);
			let plan = opt_ctx.conv_into_optd_og(&pl)?;
			let rt_config = RuntimeEnvBuilder::new();
			let session_config = SessionConfig::from_env()?
				.with_information_schema(true)
				.with_create_default_catalog_and_schema(false);
			let schem_prov = MemorySchemaProvider::new();
			for (name, table) in query.tables {
				schem_prov.register_table(name, table.clone())?;
			}
			let mem_prov = MemoryCatalogProvider::new();
			mem_prov.register_schema("public", Arc::new(schem_prov))?;
			let mem_prov_list = MemoryCatalogProviderList::new();
			mem_prov_list.register_catalog("datafusion".to_string(), Arc::new(mem_prov));
			
			let df_ctx = optd_og_datafusion_bridge::create_df_context(
				Some(session_config.clone()),
				Some(rt_config.clone()),
				Some(Arc::new(mem_prov_list)),
				false,
				false,
				true,
				None,
			).await?;
			let mut opt = df_ctx.optimizer.optimizer.lock().unwrap().take().unwrap();
			let plan = opt.heuristic_optimize(plan);
			let (gid, plan, meta) = opt.cascades_optimize(plan)?;
			opt_ctx.optimizer = Some(&opt);
			let phys_plan = opt_ctx.conv_from_optd_og(plan, meta).await?;
			
			let winfo = opt.cascades_optimizer.memo.get_group_winner(gid)
				.as_full_winner().unwrap();
			let cost = winfo.total_cost.0[COMPUTE_COST];

			// TODO actually sample alternates
			
			Ok(SampleOutput {
				best_plan: Plan::new(phys_plan, cost),
				alternates: Vec::new(),
				session: st,
			})
		}
		OptimizerBackend::Dolomite => {
			let plan = dolomite_conversion::from_df_logical(&pl)?;
			let mut opt = dolomite::cascades::CascadesOptimizer::default(plan);
			opt.rules.extend(vec![
				dolomite::rules::Join2HashJoinRule::new().into(),
				dolomite::rules::PushLimitOverProjectionRule::new().into(),
				dolomite::rules::PushLimitToTableScanRule::new().into(),
				dolomite::rules::RemoveLimitRule::new().into(),
				dolomite::rules::Scan2TableScanRule::new().into(),
			]);

			let best_plan = opt.find_best_plan()?;
			let out_plan = dolomite_conversion::to_df_logical(&best_plan)?;
			let phys_plan = st.create_physical_plan(&out_plan).await?;
			let cost = opt.memo.groups.get(&opt.memo.root_group_id)
				.unwrap().winner(&opt.required_prop)
				.unwrap().lowest_cost.0;

			// TODO actually sample alternates			
			Ok(SampleOutput {
				best_plan: Plan::new(phys_plan, cost),
				alternates: Vec::new(),
				session: st,
			})
		}		
	}
}

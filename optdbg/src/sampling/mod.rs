use std::mem::MaybeUninit;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::{SessionConfig, SessionContext};
use dolomite::cascades::CascadesOptimizer;
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use anyhow::Result;
use optd_datafusion::df_conversion::context::OptdDFContext;
use optd_og_datafusion_bridge::{OptdDfContext, OptdPlanContext};
use optd_og_core::cascades::Memo;
use optd_og_datafusion_repr::cost::COMPUTE_COST;
use async_trait::async_trait;

pub struct Plan {
	pub tree: Arc<dyn ExecutionPlan>,
	pub est_cost: f64,
}

impl Plan {
	fn new(tree: Arc<dyn ExecutionPlan>, est_cost: f64) -> Self {
		Self { tree, est_cost } 
	}
}

#[derive(Clone, Debug)]
pub enum SampleStrategy {
	MemoBased,
	RuleBased,
	HintBased
}

impl std::str::FromStr for SampleStrategy {
	type Err = &'static str;
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"memo" => Ok(SampleStrategy::MemoBased),
			"rule" => Ok(SampleStrategy::RuleBased),
			"hint" => Ok(SampleStrategy::HintBased),
			_ => Err("unknown backend")
		}
	}
}

#[async_trait]
pub trait Sampler {
	async fn get_best(&mut self, pl: LogicalPlan) -> Result<Plan>;

	/// Must be called after Sampler::get_best().
	async fn get_alternates(&mut self) -> Result<Vec<Plan>>;
}

pub struct OptdBackend {
	ctx: OptdDFContext,
	strat: SampleStrategy
}

impl OptdBackend {
	pub fn new<'a>(st: &'a SessionState, strat: SampleStrategy) -> Self {
		Self { ctx: OptdDFContext::new(st), strat } 
	}
}

#[async_trait]
impl Sampler for OptdBackend {
	async fn get_best(&mut self, _pl: LogicalPlan) -> Result<Plan> {
		todo!("new optd isn't runnable yet")
	}

	async fn get_alternates(&mut self) -> Result<Vec<Plan>> {
		todo!("new optd isn't runnable yet")
	}
}

pub struct OptdOldBackend<'a> {
	opt_ctx: OptdPlanContext<'a>,
	df_ctx: OptdDfContext,
	strat: SampleStrategy
}

impl<'a> OptdOldBackend<'a> {
	pub async fn new(
		st: &'a SessionState,
		tables: &Vec<(String, Arc<MemTable>)>,
		strat: SampleStrategy
	) -> Result<Self> {
		let opt_ctx = OptdPlanContext::new(&st);
		let rt_config = RuntimeEnvBuilder::new();
		let session_config = SessionConfig::from_env()?
			.with_information_schema(true)
			.with_create_default_catalog_and_schema(false);
		let schem_prov = MemorySchemaProvider::new();
		for (name, table) in tables {
			schem_prov.register_table(name.to_string(), table.clone())?;
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
		Ok(Self {
			df_ctx,
			opt_ctx,
			strat
		})
	}
}

#[async_trait]
impl<'a> Sampler for OptdOldBackend<'a> {
	async fn get_best(&mut self, pl: LogicalPlan) -> Result<Plan> {
		let mut opt = self.df_ctx.optimizer.optimizer.lock().unwrap().take().unwrap();
		let plan = self.opt_ctx.conv_into_optd_og(&pl)?;
		let plan = opt.heuristic_optimize(plan);
		let (gid, plan, meta) = opt.cascades_optimize(plan)?;
		// FIXME(quantumish) this is questionable :(
		// self.opt_ctx.optimizer = Some(&opt);
		let phys_plan = self.opt_ctx.conv_from_optd_og(plan, meta).await?;
		
		let winfo = opt.cascades_optimizer.memo.get_group_winner(gid)
			.as_full_winner().unwrap();
		let cost = winfo.total_cost.0[COMPUTE_COST];
		Ok(Plan::new(phys_plan, cost))
	}

	// TODO 
	async fn get_alternates(&mut self) -> Result<Vec<Plan>> {
		Ok(vec![])
	}
}

pub struct DolomiteBackend {
	opt: Option<CascadesOptimizer>,
	strat: SampleStrategy,
	state: SessionState,
}

impl DolomiteBackend {
	pub async fn new(st: SessionState, strat: SampleStrategy) -> Result<Self> {		
		Ok(Self { opt: None, strat, state: st })
	}
}

#[async_trait]
impl Sampler for DolomiteBackend {
	async fn get_best(&mut self, pl: LogicalPlan) -> Result<Plan> {
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
		let phys_plan = self.state.create_physical_plan(&out_plan).await?;
		let cost = opt.memo.groups.get(&opt.memo.root_group_id)
			.unwrap().winner(&opt.required_prop)
			.unwrap().lowest_cost.0;
		self.opt = Some(opt);
		Ok(Plan::new(phys_plan, cost))
	}

	// TODO 
	async fn get_alternates(&mut self) -> Result<Vec<Plan>> {
		Ok(vec![])
	}
}

pub struct SampleConfig;

pub struct SampleOutput {
	pub best_plan: Plan,
	pub alternates: Vec<Plan>,
	pub session: SessionState,
}

pub struct QueryInfo {
	pub plan: LogicalPlan,
	pub tables: Vec<(String, Arc<MemTable>)>,	
	pub backend: Arc<dyn Sampler>,
	pub state: SessionState,
}

pub async fn sample(mut query: QueryInfo, _cfg: SampleConfig) -> Result<SampleOutput> {
	let backend = Arc::get_mut(&mut query.backend).unwrap();
	Ok(SampleOutput {
		best_plan: backend.get_best(query.plan).await?,
		alternates: backend.get_alternates().await?,
		session: query.state,
	})
}		


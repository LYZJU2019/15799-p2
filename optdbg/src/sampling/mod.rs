use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::SessionConfig;
use dolomite::cascades::CascadesOptimizer;
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use anyhow::Result;
use itertools::Itertools;
use optd_og_datafusion_bridge::{OptdDfContext, OptdPlanContext};
use optd_og_core::cascades::Memo;
use optd_og_datafusion_repr::cost::COMPUTE_COST;
use async_trait::async_trait;
use optd_og_datafusion_repr::rules::PhysicalConversionRule;
use optd_og_datafusion_repr::DatafusionOptimizer;

// TODO find a way to compare plan properties?
fn eq_plans(a: Arc<dyn ExecutionPlan>, b: Arc<dyn ExecutionPlan>) -> bool {
	if a.name() != b.name() {
		return false;
	}
	let a_childs = a.children();
	let b_childs = b.children();
	if a_childs.len() != b_childs.len() {
		return false;
	}
	for (i,j) in a_childs.into_iter().zip(b_childs.into_iter()) {
		if !eq_plans(i.clone(), j.clone()) {
			return false;
		}
	}
	return true;
}

fn format_plan(
	f: &mut std::fmt::Formatter<'_>,
	plan: Arc<dyn ExecutionPlan>,
	indent_level: usize
) -> std::fmt::Result {
	for _ in 0..indent_level {
		write!(f, "  ")?;
	}
	writeln!(f, "{}", plan.name())?;
	for child in plan.children() {
		format_plan(f, child.clone(), indent_level + 1)?;
	}
	Ok(())
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

impl std::cmp::PartialEq for Plan {
	fn eq(&self, other: &Self) -> bool {
		eq_plans(self.tree.clone(), other.tree.clone()) && self.est_cost == other.est_cost
	}
}

impl std::cmp::Eq for Plan {}

impl std::fmt::Display for Plan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		format_plan(f, self.tree.clone(), 0)
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
	async fn get_best(&mut self, st: &SessionState, pl: LogicalPlan) -> Result<Plan>;

	/// Must be called after Sampler::get_best().
	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>>;
}

pub struct OptdBackend {
	plan: Option<LogicalPlan>,
	strat: SampleStrategy
}

impl OptdBackend {
	pub fn new(strat: SampleStrategy) -> Self {
		Self { strat, plan: None } 
	}
}

#[async_trait]
impl Sampler for OptdBackend {
	async fn get_best(&mut self, st: &SessionState, _pl: LogicalPlan) -> Result<Plan> {
		todo!("new optd isn't runnable yet")
	}

	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
		todo!("new optd isn't runnable yet")
	}
}

pub struct OptdOldBackend {
	df_ctx: OptdDfContext,
	strat: SampleStrategy,
	plan: Option<LogicalPlan>,
	opt: Option<DatafusionOptimizer>
}

impl OptdOldBackend {
	pub async fn new(
		tables: &Vec<(String, Arc<MemTable>)>,
		strat: SampleStrategy
	) -> Result<Self> {		
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
			strat,
			plan: None,
			opt: None
		})
	}

	async fn get_alts_rule(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
		let opt = self.opt.as_mut().unwrap();
		let pl = self.plan.clone().unwrap();
		let rules = opt.cascades_optimizer.rules();
		let mut out = Vec::new();

		// Pretty hacky to do powerset with the physical rules included (since we need all).
		// Cloning out of rules seems to make this not usable within an async_trait
		// so we just drain and re-add. Shouldn't be horrifically slow but is wasteful.
		for mut rs in rules.iter().powerset() {
			rs.retain(|x| x.name() != "physical_conversion");
			let temp = PhysicalConversionRule::all_conversions();
			rs.extend(temp.iter());
			
			if rs.iter().find(|r| r.name() == "join_commute_rule").is_none() ||
			   rs.iter().find(|r| r.name() == "join_assoc_rule").is_none() ||
			   rs.iter().find(|r| r.name() == "project_filter_transpose_rule").is_some()
			{
				continue;
			}
			
			// Avoid borrowing issue. Pretty hacky.
			let mut opt_ctx = OptdPlanContext::new(st);
			let plan = opt_ctx.conv_into_optd_og(&pl)?;
			let plan = opt.heuristic_optimize(plan);
		
			opt.cascades_optimizer.rules = Arc::from(
				rs.into_iter().cloned().collect::<Vec<_>>().into_boxed_slice()
			);
			opt.cascades_optimizer.step_clear();
			let (gid, opt_plan, meta) = opt.cascades_optimize(plan.clone())?;
			let winfo = opt.cascades_optimizer.memo.get_group_winner(gid)
				.as_full_winner().unwrap().clone();
			opt_ctx.optimizer = Some(&opt);
			let phys_plan = opt_ctx.conv_from_optd_og(opt_plan, meta).await?;
			let cost = winfo.total_cost.0[COMPUTE_COST];
			let phys_plan = Plan::new(phys_plan, cost);
			
			if !out.contains(&phys_plan) {
				println!("{}", phys_plan);
				out.push(phys_plan);
				println!("Have {} alternate plans", out.len());
			}
		}
		Ok(out)
	}
}

#[async_trait]
impl Sampler for OptdOldBackend {
	async fn get_best(&mut self, st: &SessionState, pl: LogicalPlan) -> Result<Plan> {
		let mut opt = self.df_ctx.optimizer.optimizer.lock().unwrap().take().unwrap();
		let mut opt_ctx = OptdPlanContext::new(st);
		let plan = opt_ctx.conv_into_optd_og(&pl)?;
		let plan = opt.heuristic_optimize(plan);
		let (gid, plan, meta) = opt.cascades_optimize(plan)?;
		let winfo = opt.cascades_optimizer.memo.get_group_winner(gid)
			.as_full_winner().unwrap().clone();
		opt_ctx.optimizer = Some(&opt);
		let phys_plan = opt_ctx.conv_from_optd_og(plan, meta).await?;
		let cost = winfo.total_cost.0[COMPUTE_COST];
		self.plan = Some(pl);
		self.opt = Some(*opt);
		let out = Plan::new(phys_plan, cost);
		println!("best is\n{out}");
		Ok(out)
	}

	// TODO 
	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
		match self.strat {
			SampleStrategy::RuleBased => self.get_alts_rule(st).await,
			_ => todo!()
		}
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
	async fn get_best(&mut self, st: &SessionState , pl: LogicalPlan) -> Result<Plan> {
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
		let out = Plan::new(phys_plan, cost);
		println!("best is\n{out}");
		Ok(out)
	}

	// TODO 
	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
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
		best_plan: backend.get_best(&query.state, query.plan).await?,
		alternates: backend.get_alternates(&query.state).await?,
		session: query.state,
	})
}		


use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::SessionConfig;
use dolomite::cascades::CascadesOptimizer as DolomiteCascadesOptimizer;
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use anyhow::Result;
use itertools::Itertools;
use optd_og_core::cascades::{ExprId, GroupId, OptimizerProperties, RelNodeContext};
use optd_og_core::logical_property::LogicalPropertyBuilderAny;
use optd_og_core::nodes::PlanNodeOrGroup;
use optd_og_datafusion_bridge::{DatafusionCatalog, OptdDfContext, OptdPlanContext};
use optd_og_core::{rules::Rule, cascades::{CascadesOptimizer as OptdCascadesOptimizer, Memo}};
use optd_og_datafusion_repr::cost::base_cost::DfStatistics;
use optd_og_datafusion_repr::cost::{AdaptiveCostModel, COMPUTE_COST};
use async_trait::async_trait;
use optd_og_datafusion_repr::plan_nodes::{DfNodeType, DfPlanNode};
use optd_og_datafusion_repr::properties::column_ref::ColumnRefPropertyBuilder;
use optd_og_datafusion_repr::properties::schema::SchemaPropertyBuilder;
use optd_og_datafusion_repr::rules;
use optd_og_datafusion_repr::DatafusionOptimizer;

use crate::common::Plan;

/// Enum representing how to decide when to stop trying out combinations of rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleBailStrategy {
	/// Stop when we run out of rule combinations.
	Never,
	/// Stop when no new plans found after n attempts (TODO @Yu this is just a placeholder)
	Inactive(usize),
	/// Stop when n new plans have been found.
	Threshold(usize),	
}

/// Enum representing how we should sample alternative plans from the query optimizer;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SampleStrategy {
	/// Create alternative plans by extracting equivalent expressions from memo table.
	MemoBased,
	/// Create alternative plans trying out different combinations of rules.
	RuleBased(RuleBailStrategy),
	/// Create alternative plans by using different optimization hints.
	HintBased
}

impl std::str::FromStr for SampleStrategy {
	type Err = &'static str;
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"memo" => Ok(SampleStrategy::MemoBased),
			"rule" => Ok(SampleStrategy::RuleBased(RuleBailStrategy::Never)),
			"hint" => Ok(SampleStrategy::HintBased),
			_ => Err("unknown backend")
		}
	}
}

/// Main trait for a query optimizer backend to implement.
#[async_trait]
pub trait Sampler {
	/// Gets the plan a query optimizer would choose for a logical plan.
	async fn get_best(&mut self, st: &SessionState, pl: LogicalPlan) -> Result<Plan>;

	/// Gets the other possible plans a query optimizer could choose for a logical plan.
	/// Must be called after Sampler::get_best(). Output Must not contain the result of Sampler::get_best().
	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>>;
}

/// Backend for new optd.
pub struct OptdBackend {
	/// Plan given in get_best().
	plan: Option<LogicalPlan>,
	/// Strategy to use when sampling.
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

/// Backend for old optd.
pub struct OptdOldBackend {
	/// Datafusion context. Needed to hold optimizer + catalog.
	df_ctx: OptdDfContext,
	/// Sampling strategy to use.
	strat: SampleStrategy,
	/// Plan passed in get_best().
	plan: Option<LogicalPlan>,
	/// Optimizer resulting from take() operation in get_best(). 
	opt: Option<DatafusionOptimizer>,
	/// Physical plan returned by get_best().
	best: Option<Plan>,
}

impl OptdOldBackend {
	pub async fn new(
		tables: Arc<dyn SchemaProvider>,
		strat: SampleStrategy
	) -> Result<Self> {
		if strat == SampleStrategy::HintBased {
			return Err(anyhow::anyhow!("optd-old doesn't support optimization hints"));
		}
		
		let rt_config = RuntimeEnvBuilder::new();
		let session_config = SessionConfig::from_env()?
			.with_information_schema(true)
			.with_create_default_catalog_and_schema(false);
		let mem_prov = MemoryCatalogProvider::new();
		mem_prov.register_schema("public", tables)?;
		let mem_prov_list = MemoryCatalogProviderList::new();
		mem_prov_list.register_catalog("datafusion".to_string(), Arc::new(mem_prov));
		
		let df_ctx = optd_og_datafusion_bridge::create_df_context(
			Some(session_config.clone()),
			Some(rt_config.clone()),
			Some(Arc::new(mem_prov_list)),
			false,
			false,
			false,
			None,
		).await?;
		Ok(Self {
			df_ctx,
			strat,
			plan: None,
			opt: None,
			best: None
		})
	}

	#[async_recursion]
	async fn get_est_cards(
		node: Arc<DfPlanNode>,
		opt: &DatafusionOptimizer,
		cards: &mut Vec<f64>,
	) -> optd_og_core::cost::Statistics {
		let mut children = Vec::new();
		for i in node.children.iter().rev() {
			let PlanNodeOrGroup::PlanNode(p) = i else {
				panic!("shouldn't see group after optimization");
			};
			children.push(Self::get_est_cards(p.clone(), opt, cards).await);
		}
		let children2: Vec<_> = (0..children.len()).rev().map(|x| &children[x]).collect();
		let stats = opt.cascades_optimizer.cost.derive_statistics(
			&node.typ, &node.predicates, children2.as_slice(),
			RelNodeContext {
				group_id: GroupId(0), expr_id: ExprId(0), children_group_ids: vec![]
			},
			&opt.cascades_optimizer,
		);
		cards.push(stats.0.downcast_ref::<DfStatistics>().unwrap().row_cnt);
		stats
	}

	/// Implements rule-based sampling for optd-old backend.
	// TODO Be a lot smarter about this: can pre-filter rules for applicability,
	// can sort rules in the thresholding case, can hash trees for speedier equality checks,
	// can probably see if this doing anything else dumb, Generally hacky.
	//
	// TODO implement support for RuleBailStrategy::Inactive or something	
	async fn get_alts_rule(
		&mut self,
		st: &SessionState,
		bail: RuleBailStrategy
	) -> Result<Vec<Plan>> {
		let opt = self.opt.as_mut().unwrap();
		let pl = self.plan.clone().unwrap();
		let mut out = Vec::new();

		let defaults: Vec<Arc<dyn Rule<DfNodeType, OptdCascadesOptimizer<DfNodeType>>>> = vec![
			Arc::new(rules::FilterInnerJoinTransposeRule::new()),
			Arc::new(rules::FilterSortTransposeRule::new()),
			Arc::new(rules::FilterAggTransposeRule::new()),
			Arc::new(rules::HashJoinRule::new()),
			Arc::new(rules::ProjectionPullUpJoin::new()),
			Arc::new(rules::EliminateProjectRule::new()),
			Arc::new(rules::ProjectMergeRule::new()),
			Arc::new(rules::EliminateLimitRule::new()),
			Arc::new(rules::EliminateJoinRule::new()),
			Arc::new(rules::EliminateFilterRule::new()),
			Arc::new(rules::ProjectFilterTransposeRule::new()),
		];
		
		let needed: Vec<Arc<dyn Rule<DfNodeType, OptdCascadesOptimizer<DfNodeType>>>> = vec![
			Arc::new(rules::JoinCommuteRule::new()),
			Arc::new(rules::JoinAssocRule::new()),
		];
		
		for mut rs in defaults.iter().powerset() {
			let temp = rules::PhysicalConversionRule::all_conversions();
			rs.extend(temp.iter());
			rs.extend(needed.iter());

			// println!("Trying out {:?}",
			// 		 rs.clone().into_iter().map(|x| x.name())
			// 		 .filter(|x| *x != "physical_conversion").collect::<Vec<_>>());
			// optd will dump some info somewhere if budget is exhausted.
			// NOTE: this blocks all println! calls! remember me when debugging!!
			// let gag = gag::Gag::stdout().unwrap();
			// Avoid borrowing issue. Pretty hacky.
			let mut opt_ctx = OptdPlanContext::new(st);
			let plan = opt_ctx.conv_into_optd_og(&pl)?;
			let plan = opt.heuristic_optimize(plan);

			let catalog = Arc::new(DatafusionCatalog::new(self.df_ctx.catalog.clone()));
			let optim = OptdCascadesOptimizer::new_with_options(
                rs.clone().into_iter().cloned().collect::<Vec<_>>(),
                Box::new(AdaptiveCostModel::new(50)),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone()))
                        as Box<dyn LogicalPropertyBuilderAny<DfNodeType>>,
                    Box::new(ColumnRefPropertyBuilder::new(catalog.clone()))
                        as Box<dyn LogicalPropertyBuilderAny<DfNodeType>>,
                ]
                .into(),
                OptimizerProperties {
                    panic_on_budget: false,
                    partial_explore_iter: Some(1 << 18),
                    partial_explore_space: Some(1 << 14),
                    disable_pruning: false,
                    enable_tracing: false,
                },
            );

			opt.cascades_optimizer = optim;

				
			opt.cascades_optimizer.rules = Arc::from(
				rs.into_iter().cloned().collect::<Vec<_>>().into_boxed_slice()
			);
						
			let (gid, opt_plan, meta) = opt.cascades_optimize(plan.clone())?;

			let mut cards = Vec::new();
			Self::get_est_cards(opt_plan.clone(), &opt, &mut cards).await;
			cards.reverse();
			
			let winfo = opt.cascades_optimizer.memo.get_group_winner(gid)
				.as_full_winner().unwrap().clone();
			opt_ctx.optimizer = Some(&opt);

			let phys_plan = opt_ctx.conv_from_optd_og(opt_plan, meta).await?;
			let cost = winfo.total_cost.0[COMPUTE_COST];
			let phys_plan = Plan::new(phys_plan, cost, cards);			

			// drop(gag);
			if !out.contains(&phys_plan) && *self.best.as_ref().unwrap() != phys_plan {
				println!("{}", phys_plan);
				out.push(phys_plan);
				if let RuleBailStrategy::Threshold(thres) = bail {
					if out.len() == thres {
						break;
					}
				}
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
		let mut cards = Vec::new();
		Self::get_est_cards(plan.clone(), &opt, &mut cards).await;
		cards.reverse();
		
		opt_ctx.optimizer = Some(&opt);
		let phys_plan = opt_ctx.conv_from_optd_og(plan, meta).await?;
		let cost = winfo.total_cost.0[COMPUTE_COST];
		self.plan = Some(pl);
		self.opt = Some(*opt);
		let out = Plan::new(phys_plan, cost, cards);
		self.best = Some(out.clone());
		println!("best is\n{out}");
		Ok(out)
	}

	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
		match &self.strat {
			SampleStrategy::RuleBased(t) => self.get_alts_rule(st, *t).await,
			SampleStrategy::MemoBased => todo!(),
			_ => unreachable!()
		}
	}
}

/// Backend for the datafusion-dolomite optimizer.
pub struct DolomiteBackend {
	/// Optimizer created when get_best() is called. 
	opt: Option<DolomiteCascadesOptimizer>,
	/// Sampling strategy to use.
	strat: SampleStrategy,
	/// State of datafusion session.
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
		// We make the optimizer down here because it needs to take in a plan.
		let mut opt = DolomiteCascadesOptimizer::default(plan);
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
		let out = Plan::new(phys_plan, cost, vec![]);
		println!("best is\n{out}");
		Ok(out)
	}

	async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
		todo!()
	}
}

// Placeholder type for when any settings are introduced. In practice this probably isn't needed.
pub struct SampleConfig;

/// Output of sampler.
pub struct SampleOutput {
	/// Plan chosen by the query optimizer.
	pub best_plan: Plan,
	/// Alternative plans not chosen by the query optimizer.
	pub alternates: Vec<Plan>,
	/// Datafusion session.
	pub session: SessionState,
	/// Schemas of each table.
	pub tables: Arc<dyn SchemaProvider>,
}

/// Input to sampler.
pub struct QueryInfo {
	/// Logical plan of query to optimize.
	pub plan: LogicalPlan,	
	pub tables: Arc<dyn SchemaProvider>,
	pub backend: Arc<dyn Sampler>,
	pub state: SessionState,
}

pub async fn sample(mut query: QueryInfo, _cfg: SampleConfig) -> Result<SampleOutput> {
	let backend = Arc::get_mut(&mut query.backend).unwrap();
	Ok(SampleOutput {
		best_plan: backend.get_best(&query.state, query.plan).await?,
		alternates: backend.get_alternates(&query.state).await?,
		session: query.state,
		tables: query.tables,
	})
}		


use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::{Stream, StreamExt};

use anyhow::Result;
use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    SchemaProvider,
};
use datafusion::common::HashSet;
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use dolomite::cascades::CascadesOptimizer as DolomiteCascadesOptimizer;
use dolomite::optimizer::Optimizer;
use itertools::Itertools;
use optd_og_core::cascades::{ExprId, GroupId};
use optd_og_core::cost::Cost;
use optd_og_core::nodes::{PlanNode, PlanNodeMeta, PlanNodeMetaMap, PlanNodeOrGroup};
use optd_og_core::{
    cascades::{CascadesOptimizer as OptdCascadesOptimizer, Memo},
    rules::Rule,
};
use optd_og_datafusion_bridge::{DatafusionCatalog, OptdDfContext, OptdPlanContext};
use optd_og_datafusion_repr::DatafusionOptimizer;
use optd_og_datafusion_repr::cost::COMPUTE_COST;
use optd_og_datafusion_repr::cost::base_cost::DfStatistics;
use optd_og_datafusion_repr::plan_nodes::{ArcDfPlanNode, DfNodeType};
use optd_og_datafusion_repr::rules;
use optd_og_datafusion_repr_adv_cost::adv_stats::stats::DataFusionBaseTableStats;
use optd_og_datafusion_repr_adv_cost::adv_stats::stats::DataFusionPerTableStats;
use optd_og_datafusion_repr_adv_cost::new_physical_adv_cost;
use rayon::prelude::*;

use crate::common::Plan;

mod matcher;

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
    HintBased,
}

impl std::str::FromStr for SampleStrategy {
    type Err = &'static str;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memo" => Ok(SampleStrategy::MemoBased),
            "rule" => Ok(SampleStrategy::RuleBased(RuleBailStrategy::Never)),
            "hint" => Ok(SampleStrategy::HintBased),
            _ => Err("unknown backend"),
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
    strat: SampleStrategy,
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
    best_cascades: Option<(GroupId, ArcDfPlanNode, PlanNodeMetaMap)>,
    stats: Option<DataFusionBaseTableStats>,
}

/// Taken from optd-perfbench (all credit to Alexis Schlomer).
/// (not super easy to pull out of optd, so copy and paste :/)
fn build_batch_reader(
    tbl_fpath: PathBuf,
    num_row_groups: usize,
) -> impl FnOnce() -> Vec<ParquetRecordBatchReader> {
    move || {
        let groups: Vec<ParquetRecordBatchReader> = (0..num_row_groups)
            .map(|group_num| {
                let tbl_file = std::fs::File::open(tbl_fpath.clone()).expect("Failed to open file");
                let metadata = ArrowReaderMetadata::load(&tbl_file, Default::default()).unwrap();

                ParquetRecordBatchReaderBuilder::new_with_metadata(
                    tbl_file.try_clone().unwrap(),
                    metadata.clone(),
                )
                .with_row_groups(vec![group_num])
                .build()
                .unwrap()
            })
            .collect();

        groups
    }
}

/// Taken from optd-perfbench (all credit to Alexis Schlomer).
/// (not super easy to pull out of optd, so copy and paste :/)
fn gen_base_stats(tbl_paths: Vec<(String, PathBuf)>) -> anyhow::Result<DataFusionBaseTableStats> {
    let base_table_stats = Mutex::new(DataFusionBaseTableStats::default());
    let now = std::time::Instant::now();

    tbl_paths.par_iter().for_each(|(tbl_name, tbl_fpath)| {
        let start = std::time::Instant::now();

        // We get the schema from the Parquet file, to ensure there's no divergence between
        // the context and the file we are going to read.
        // Further rounds of refactoring should adapt the entry point of stat gen.
        let tbl_file = std::fs::File::open(tbl_fpath).expect("Failed to open file");
        let parquet =
            ParquetRecordBatchReaderBuilder::try_new(tbl_file.try_clone().unwrap()).unwrap();
        let schema = parquet.schema();

        let nb_cols = schema.fields().len();
        let single_cols = (0..nb_cols).map(|v| vec![v]).collect::<Vec<_>>();

        let stats_result = DataFusionPerTableStats::from_record_batches(
            build_batch_reader(tbl_fpath.clone(), parquet.metadata().num_row_groups()),
            build_batch_reader(tbl_fpath.clone(), parquet.metadata().num_row_groups()),
            single_cols,
            schema.clone(),
        );

        if let Ok(per_table_stats) = stats_result {
            let mut stats = base_table_stats.lock().unwrap();
            stats.insert(tbl_name.to_string(), per_table_stats);
        }

        println!(
            "Table {:?} took in total {:?}...",
            tbl_name,
            start.elapsed()
        );
    });

    println!("Total execution time {:?}...", now.elapsed());

    let stats = base_table_stats.into_inner();
    let l = stats.unwrap();

    Ok(l)
}

impl OptdOldBackend {
    pub async fn new(
        tables: Arc<dyn SchemaProvider>,
        table_files: Vec<(String, PathBuf)>,
        strat: SampleStrategy,
        adv: bool,
    ) -> Result<Self> {
        if strat == SampleStrategy::HintBased {
            return Err(anyhow::anyhow!(
                "optd-old doesn't support optimization hints"
            ));
        }

        let rt_config = RuntimeEnvBuilder::new();
        let session_config = SessionConfig::from_env()?
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false);
        let mem_prov = MemoryCatalogProvider::new();
        mem_prov.register_schema("public", tables)?;
        let mem_prov_list = MemoryCatalogProviderList::new();
        mem_prov_list.register_catalog("datafusion".to_string(), Arc::new(mem_prov));

        let stats = if adv {
            Some(gen_base_stats(table_files)?)
        } else {
            None
        };

        let df_ctx = optd_og_datafusion_bridge::create_df_context(
            Some(session_config.clone()),
            Some(rt_config.clone()),
            Some(Arc::new(mem_prov_list)),
            false,
            true,
            adv,
            stats.clone(),
        )
        .await?;
        Ok(Self {
            df_ctx,
            strat,
            plan: None,
            opt: None,
            best: None,
            best_cascades: None,
            stats,
        })
    }

    #[async_recursion]
    async fn get_costs_and_cards(
        node: GroupId,
        opt: &DatafusionOptimizer,
        costs: &mut Vec<f64>,
        cards: &mut Vec<f64>,
    ) {
        let winfo = opt
            .cascades_optimizer
            .memo
            .get_group_winner(node)
            .as_full_winner()
            .unwrap()
            .clone();

        costs.push(winfo.total_cost.0[COMPUTE_COST]);
        cards.push(
            winfo
                .statistics
                .0
                .downcast_ref::<DfStatistics>()
                .unwrap()
                .row_cnt,
        );

        let expr = opt.cascades_optimizer.memo.get_expr_memoed(winfo.expr_id);
        for child in &expr.children {
            Self::get_costs_and_cards(*child, opt, costs, cards).await;
        }
    }

    fn get_alts_help(
        opt: &DatafusionOptimizer,
        gid: GroupId,
        visited: &mut HashSet<ExprId>,
        fake_meta: &mut PlanNodeMetaMap,
        physical_expr_count: &mut HashMap<GroupId, usize>,
    ) -> Vec<ArcDfPlanNode> {
        let mut out = Vec::new();
        let exprs = &opt.cascades_optimizer.memo.get_group(gid).group_exprs;

        // println!("Group {gid} has {} expressions", exprs.len());
        let mut physical_cnt = 0;
        for expr_id in exprs {
            if visited.contains(expr_id) {
                continue;
            }
            visited.insert(*expr_id);
            let expr = opt.cascades_optimizer.memo.get_expr_memoed(*expr_id);
            if !matches!(
                expr.typ,
                DfNodeType::PhysicalAgg
                    | DfNodeType::PhysicalEmptyRelation
                    | DfNodeType::PhysicalProjection
                    | DfNodeType::PhysicalScan
                    | DfNodeType::PhysicalFilter
                    | DfNodeType::PhysicalSort
                    | DfNodeType::PhysicalNestedLoopJoin(_)
                    | DfNodeType::PhysicalHashJoin(_)
                    | DfNodeType::PhysicalLimit
            ) {
                continue;
            }
            physical_cnt += 1;
            let mut children: Vec<Vec<ArcDfPlanNode>> = Vec::with_capacity(expr.children.len());
            for child in &expr.children {
                // println!("Expr w/ id {expr_id} (aka {}) is has child in group {child}", expr.typ)
                let child_alts =
                    Self::get_alts_help(opt, *child, visited, fake_meta, physical_expr_count);
                children.push(child_alts);
                // println!("Child group {child} has {} alternatives", len);
            }

            if children.is_empty() {
                out.push(Arc::new(optd_og_core::nodes::PlanNode {
                    typ: expr.typ.clone(),
                    children: vec![],
                    predicates: expr
                        .predicates
                        .iter()
                        .map(|x| opt.cascades_optimizer.memo.get_pred(*x))
                        .collect(),
                }));
            } else {
                let iter = children.iter().multi_cartesian_product();
                for children in iter {
                    let children = children
                        .into_iter()
                        .map(|x| PlanNodeOrGroup::PlanNode(x.clone()))
                        .collect();
                    let thing = Arc::new(optd_og_core::nodes::PlanNode {
                        typ: expr.typ.clone(),
                        children,
                        predicates: expr
                            .predicates
                            .iter()
                            .map(|x| opt.cascades_optimizer.memo.get_pred(*x))
                            .collect(),
                    });
                    // PUSH CHILDREN INTO META
                    Self::insert_with_children(&thing, gid, fake_meta);
                    out.push(thing.clone());
                }
            }
        }

        physical_expr_count
            .entry(gid)
            .and_modify(|x| *x += physical_cnt)
            .or_insert(physical_cnt);

        // println!("Group {gid} has {} physical expressions in memo", memo_cnt);
        out
    }

    fn insert_with_children(
        node: &Arc<PlanNode<DfNodeType>>,
        gid: GroupId,
        fake_meta: &mut PlanNodeMetaMap,
    ) {
        fake_meta.insert(
            node.as_ref() as *const _ as usize,
            PlanNodeMeta {
                group_id: gid,
                weighted_cost: 0.0,
                cost: Cost(vec![]),
                stat: Arc::new(optd_og_core::cost::Statistics(Box::new(DfStatistics {
                    row_cnt: 0.0,
                }))),
                cost_display: "".to_string(),
                stat_display: "".to_string(),
            },
        );

        for child in &node.children {
            if let PlanNodeOrGroup::PlanNode(child_node) = child {
                Self::insert_with_children(child_node, gid, fake_meta);
            }
        }
    }

    async fn get_alts_memo(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
        let opt = self.opt.as_ref().unwrap();
        let (gid, _, _) = self.best_cascades.take().unwrap();
        let mut set = HashSet::new();
        let mut fake_meta = HashMap::new();
        let mut physical_expr_count = HashMap::new();
        let plans =
            Self::get_alts_help(opt, gid, &mut set, &mut fake_meta, &mut physical_expr_count);

        println!("Found {} alternates", plans.len());
        let mut opt_ctx = OptdPlanContext::new(st);
        opt_ctx.conv_into_optd_og(&self.plan.clone().unwrap())?;
        opt_ctx.optimizer = Some(&opt);
        let mut out = Vec::new();
        for plan in plans {
            // println!("{plan}");
			// println!("waaa")
            let phys_plan = opt_ctx.conv_from_optd_og(plan, fake_meta.clone()).await?;
			let sz = crate::common::plan_size(phys_plan.clone());
            out.push(Plan::new(phys_plan, vec![0.0; sz], vec![0.0; sz]))
        }
        Ok(out)
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
        bail: RuleBailStrategy,
    ) -> Result<Vec<Plan>> {
        let opt = self.opt.as_mut().unwrap();
        let pl = self.plan.clone().unwrap();
        let mut out = HashSet::new();

        let defaults: Vec<Arc<dyn Rule<DfNodeType, OptdCascadesOptimizer<DfNodeType>>>> = vec![
            Arc::new(rules::FilterInnerJoinTransposeRule::new()),
            Arc::new(rules::FilterSortTransposeRule::new()),
            Arc::new(rules::FilterAggTransposeRule::new()),
            Arc::new(rules::ProjectionPullUpJoin::new()),
            Arc::new(rules::EliminateProjectRule::new()),
            Arc::new(rules::ProjectMergeRule::new()),
            Arc::new(rules::EliminateLimitRule::new()),
            Arc::new(rules::EliminateJoinRule::new()),
            Arc::new(rules::EliminateFilterRule::new()),
            Arc::new(rules::ProjectFilterTransposeRule::new()),
            Arc::new(rules::HashJoinRule::new()),
        ];

        let needed: Vec<Arc<dyn Rule<DfNodeType, OptdCascadesOptimizer<DfNodeType>>>> = vec![
            Arc::new(rules::JoinCommuteRule::new()),
            Arc::new(rules::JoinAssocRule::new()),
        ];

        let mut opt_ctx = OptdPlanContext::new(st);
        let plan = opt_ctx.conv_into_optd_og(&pl)?;

        let plan = opt.heuristic_optimize(plan);

        // find all applicable rules
        let applicable_rules: Vec<Arc<dyn Rule<DfNodeType, OptdCascadesOptimizer<DfNodeType>>>> =
            defaults
                .iter()
                .filter(|rule| {
                    matcher::is_rule_applicable::<DfNodeType, OptdCascadesOptimizer<DfNodeType>>(
                        rule.as_ref(),
                        &plan,
                    )
                })
                .cloned()
                .collect::<Vec<_>>();

        println!(
            "Found {} out of {} applicable rules",
            applicable_rules.len(),
            defaults.len()
        );

        let catalog = Arc::new(DatafusionCatalog::new(self.df_ctx.catalog.clone()));
        let convs = rules::PhysicalConversionRule::all_conversions();

        for mut rs in applicable_rules.iter().powerset() {
            rs.extend(convs.iter());
            rs.extend(needed.iter());

            // NOTE: this blocks all println! calls! remember me when debugging!!
            let gag = gag::Gag::stdout().unwrap();

            // Rebuilding optimizer is probably not necessary but it was
            // the first thing that started working.
            let mut opt = if let Some(stats) = &self.stats {
                new_physical_adv_cost(catalog.clone(), stats.clone(), false)
            } else {
                DatafusionOptimizer::new_physical(catalog.clone(), false)
            };

            opt.cascades_optimizer.rules = Arc::from(
                rs.into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            );

            let (gid, opt_plan, meta) = opt.cascades_optimize(plan.clone())?;

            let mut cards = Vec::new();
            let mut costs = Vec::new();
            Self::get_costs_and_cards(gid, &opt, &mut costs, &mut cards).await;

            let mut opt_ctx = OptdPlanContext::new(st);
            // This changes some state within the optimizer somewhere. Dunno what, though.
            opt_ctx.conv_into_optd_og(&pl)?;
            opt_ctx.optimizer = Some(&opt);
            let phys_plan = opt_ctx.conv_from_optd_og(opt_plan, meta).await?;
            let phys_plan = Plan::new(phys_plan, costs, cards);

            drop(gag);

            if !out.contains(&phys_plan) && *self.best.as_ref().unwrap() != phys_plan {
                println!("{}", phys_plan);
                out.insert(phys_plan);
                if let RuleBailStrategy::Threshold(thres) = bail {
                    if out.len() == thres {
                        break;
                    }
                }
                println!("Have {} alternate plans", out.len());
            }
        }

        Ok(out.iter().cloned().collect_vec())
    }
}

#[async_trait]
impl Sampler for OptdOldBackend {
    async fn get_best(&mut self, st: &SessionState, pl: LogicalPlan) -> Result<Plan> {
        let mut opt = self
            .df_ctx
            .optimizer
            .optimizer
            .lock()
            .unwrap()
            .take()
            .unwrap();
        let mut opt_ctx = OptdPlanContext::new(st);
        let plan = opt_ctx.conv_into_optd_og(&pl)?;
        let plan = opt.heuristic_optimize(plan);
        let out = opt.cascades_optimize(plan)?;
        self.best_cascades = Some(out.clone());
        let (gid, plan, meta) = out;
        let mut cards = Vec::new();
        let mut costs = Vec::new();
        Self::get_costs_and_cards(gid, &opt, &mut costs, &mut cards).await;

        opt_ctx.optimizer = Some(&opt);
        let phys_plan = opt_ctx.conv_from_optd_og(plan, meta).await?;
        self.plan = Some(pl);
        self.opt = Some(*opt);
        let out = Plan::new(phys_plan, costs, cards);
        self.best = Some(out.clone());

        // println!("best is\n{out}");
        Ok(out)
    }

    async fn get_alternates(&mut self, st: &SessionState) -> Result<Vec<Plan>> {
        match &self.strat {
            SampleStrategy::RuleBased(t) => self.get_alts_rule(st, *t).await,
            SampleStrategy::MemoBased => self.get_alts_memo(st).await,
            _ => unreachable!(),
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
        Ok(Self {
            opt: None,
            strat,
            state: st,
        })
    }
}

#[async_trait]
impl Sampler for DolomiteBackend {
    async fn get_best(&mut self, st: &SessionState, pl: LogicalPlan) -> Result<Plan> {
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
        let cost = opt
            .memo
            .groups
            .get(&opt.memo.root_group_id)
            .unwrap()
            .winner(&opt.required_prop)
            .unwrap()
            .lowest_cost
            .0;
        self.opt = Some(opt);
        let out = Plan::new(phys_plan, vec![cost], vec![0.0]);
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
	pub name: String,
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
	pub name: String, 
    /// Logical plan of query to optimize.
    pub plan: LogicalPlan,
    pub tables: Arc<dyn SchemaProvider>,
    pub backend: Arc<dyn Sampler>,
    pub state: SessionState,
}

pub fn sample(
	queries: impl Stream<Item = QueryInfo>,
	_cfg: SampleConfig
) -> impl Stream<Item = SampleOutput> {
	queries.then(|mut query| async move {
		let backend = Arc::get_mut(&mut query.backend).unwrap();
		let best = backend.get_best(&query.state, query.plan).await?;
		let alts = backend.get_alternates(&query.state).await?;
		// let alts = Vec::new();
		
		println!("Found {} alternatives", alts.len());
		
		Ok(SampleOutput {
			name: query.name,
			best_plan: best,
			alternates: alts,
			session: query.state,
			tables: query.tables,
		})
	}).filter_map(|x: Result<SampleOutput>| async {
		x.ok()
	})
}

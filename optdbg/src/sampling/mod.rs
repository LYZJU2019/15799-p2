use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::{SessionConfig, SessionContext};
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use anyhow::Result;
use optd_og_datafusion_bridge::OptdPlanContext;

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

pub async fn sample(query: String, cfg: SampleConfig) -> Result<SampleOutput> {
	let config = SessionConfig::default();
    let df_ctx = SessionContext::new_with_config(config);
	let df = df_ctx.sql(&query).await?;
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
			let _plan = opt_ctx.conv_into_optd_og(&pl);			
			todo!()
		}
		OptimizerBackend::Dolomite => {
			let plan = dolomite_conversion::from_df_logical(&pl)?;
			let opt = dolomite::cascades::CascadesOptimizer::default(plan);
			let best_plan = opt.find_best_plan()?;
			let out_plan = dolomite_conversion::to_df_logical(&best_plan)?;
			let phys_plan = st.create_physical_plan(&out_plan).await?;
			Ok(SampleOutput {
				best_plan: Plan::new(phys_plan, 0.0),
				alternates: Vec::new(),
				session: st,
			})
		}		
	}
}

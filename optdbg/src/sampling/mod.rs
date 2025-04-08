use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::{SessionConfig, SessionContext};
use dolomite::optimizer::Optimizer;
use datafusion_dolomite_integration::conversion as dolomite_conversion;
use optd_datafusion::df_conversion::context::OptdDFContext;
use anyhow::Result;

#[derive(Clone, Debug)]
pub enum OptimizerBackend {
	Optd,
	Dolomite,
}

impl std::str::FromStr for OptimizerBackend {
	type Err = &'static str;
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"optd" => Ok(OptimizerBackend::Optd),
			"dolomite" => Ok(OptimizerBackend::Dolomite),
			_ => Err("unknown backend")
		}
	}
}

pub struct SampleOutput {
	pub best_plan: Arc<dyn ExecutionPlan>,
	pub alternates: Vec<Arc<dyn ExecutionPlan>>,
	pub session: SessionState,
}

pub async fn sample(query: String, opt: OptimizerBackend) -> Result<SampleOutput> {
	let config = SessionConfig::default();
    let df_ctx = SessionContext::new_with_config(config);
	let df = df_ctx.sql(&query).await?;
	let (st, pl) = df.into_parts();
	match opt {
		OptimizerBackend::Optd => {
			let mut opt_ctx = OptdDFContext::new(&st);
			let _plan = opt_ctx.df_to_optd_relational(&pl);
			// use mockmemo?
			todo!()
		},
		OptimizerBackend::Dolomite => {
			let plan = dolomite_conversion::from_df_logical(&pl)?;
			let opt = dolomite::cascades::CascadesOptimizer::default(plan);
			let best_plan = opt.find_best_plan()?;
			let out_plan = dolomite_conversion::to_df_logical(&best_plan)?;
			let phys_plan = st.create_physical_plan(&out_plan).await?;
			Ok(SampleOutput {
				best_plan: phys_plan,
				alternates: Vec::new(),
				session: st,
			})
		}		
	}
}

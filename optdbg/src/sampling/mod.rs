use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::{SessionConfig, SessionContext};
use optd_datafusion::df_conversion::context::OptdDFContext;
use anyhow::Result;

pub enum OptimizerBackend {
	Optd,
	DataFusion,
}

pub async fn sample(query: String, opt: OptimizerBackend) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
	let config = SessionConfig::default();
    let df_ctx = SessionContext::new_with_config(config);
	let df = df_ctx.sql(&query).await?;
	match opt {
		OptimizerBackend::Optd => {
			let (st, pl) = df.into_parts();
			let mut opt_ctx = OptdDFContext::new(&st);
			let _plan = opt_ctx.df_to_optd_relational(&pl);
			todo!()
		},
		OptimizerBackend::DataFusion => {
			let plan = df.logical_plan();
			let plan = datafusion_dolomite_integration::conversion::from_df_logical(&plan)?;
			let opt = dolomite::cascades::CascadesOptimizer::default(plan);
			todo!();
		}		
	}
}

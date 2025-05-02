//!
//! Actual benchmarking script called by the benchmark process.
//! This is it's own target because we need process isolation: oftentimes bad workloads
//! are *so* bad that they will OOM and get the entire process killed (which sucks for us).
//!
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, Duration};

use futures::StreamExt;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_proto::bytes::physical_plan_from_bytes;

use clap::Parser;
use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};

async fn time_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
) -> anyhow::Result<(usize, Duration)> {
	let (node, ctx) = (node.clone(), ctx.clone());
	let before = Instant::now();
	let out = execute_stream(node, ctx);
	let res = out?
		.filter_map(|x| async { x.ok() })
		// slight paranoia about unnecessary memory usage
		.then(|ref x| { let rows = x.num_rows(); async move { rows }})
		.fold(0, |acc, x| async move { acc + x }).await;
	let after = Instant::now();
	Ok((res, after-before))
}

/// Benchmark runner.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the plan to benchmark 
    #[arg(short, long)]
    plan_path: String,

	/// Path to place output
	#[arg(short, long)]
    output_path: String,

	/// Path to schema info 
	#[arg(short, long)]
    schemas_path: String,

	/// Path to data directory
	#[arg(short = 'd', long, default_value = "./tpch-data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();

	let schema_bytes = std::fs::read(Path::new(&args.schemas_path))?;
	let schemas: Vec<(String, Schema)> = serde_json::from_slice(&schema_bytes)?;
	
	let df_ctx = SessionContext::new();
	for tableref in schemas {
		let options = ParquetReadOptions::new().schema(&tableref.1);
		let path = format!("{}/{}.parquet", args.data_dir, tableref.0);
		let table_path = std::path::Path::new(&path).canonicalize()?;
		df_ctx.register_parquet(
			tableref.0,
			table_path.to_str().unwrap(), options
		).await?;
	}

	let plan_bytes = std::fs::read(Path::new(&args.plan_path))?;
	let plan = physical_plan_from_bytes(&plan_bytes, &df_ctx)?;
	let res = time_subplan(plan, df_ctx.task_ctx()).await?;
		
	let serialized = serde_json::to_string(&res)?;
	std::fs::write(&args.output_path, &serialized)?;
	
	Ok(())
}

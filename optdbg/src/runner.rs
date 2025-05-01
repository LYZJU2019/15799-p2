//!
//! Actual benchmarking script called by the benchmark process.
//! This is it's own target because we need process isolation: oftentimes bad workloads
//! are *so* bad that they will OOM and get the entire process killed (which sucks for us).
//!
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, Duration};

use futures::{StreamExt};
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_proto::bytes::physical_plan_from_bytes;
use optdbg::benchmark::BenchmarkConfig;

use async_recursion::async_recursion;
use clap::Parser;
use datafusion::arrow::array::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::{collect, ExecutionPlan}};
use optdbg::common::{MeasureError, PlanMeasurements};

async fn time_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	timeout: Option<Duration>,
) -> anyhow::Result<Option<(usize, Duration)>> {
	let (result_tx, result_rx) = tokio::sync::oneshot::channel();
	let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
	if let Some(timeout) = timeout {
		let (node, ctx) = (node.clone(), ctx.clone());
		let blocking_task = tokio::task::spawn(async move {
			let before = Instant::now();
			let out = execute_stream(node, ctx);
			if let Ok(out) = out { 
				let rows: Vec<_> = out
					.filter_map(|x| async { x.ok() })
					.then(|x| async move { x.num_rows() }).collect().await;
				let res = rows.into_iter().sum();
				let after = Instant::now();
				let _ = result_tx.send((Some(res), after-before));
			} else {
				let after = Instant::now();
				let _ = result_tx.send((None, after-before));
			}
		});
		tokio::spawn(async move {
			tokio::time::sleep(timeout).await;
			let _ = cancel_tx.send(());
		});
		tokio::select! {
			result = result_rx => {
				let (res, time) = result?;
				if let Some(res) = res { 
					Ok(Some((res, time)))
				} else {
					Err(anyhow::anyhow!("exec err"))
				}
			}
			_ = cancel_rx => {
				Ok(None)
			}
		}
	} else {
		let (node, ctx) = (node.clone(), ctx.clone());
		let before = Instant::now();
		let out = execute_stream(node, ctx);
		let rows: Vec<_> = out?
			.filter_map(|x| async { x.ok() })
			.then(|x| async move { x.num_rows() }).collect().await;
		let res = rows.into_iter().sum();
		let after = Instant::now();
		Ok(Some((res, after-before)))
	}
}

// /// Recursively populate cardinality and runtime arrays.
// // TODO need to be a *lot* more rigorous for the actual benchmarking here.
// // one option is to try and integrate an existing optimizer like criterion
// // or divan. Both of these don't really support usage as a library though...
// // Doing this properly is an easy way to surpass TAQO.
// //
// // The other major TODO (this is long term) is to support the `fast` option 
// // and implement the optimization in www.vldb.org/pvldb/vol2/vldb09-294.pdf
// #[async_recursion]
// async fn measure_subplan(
// 	node: Arc<dyn ExecutionPlan>,
// 	ctx: Arc<TaskContext>,
// 	cfg: &BenchmarkConfig,
// 	cards: &mut Vec<Result<usize, MeasureError>>,
// 	times: &mut Vec<Result<Duration, MeasureError>>,
// ) -> anyhow::Result<()> {
// 	println!("Timing subplan");
// 	optdbg::common::dump_plan(node.clone(), 0);	
// 	if let Some((batches, time)) = time_subplan(node.clone(), ctx.clone(), cfg.timeout).await? {
// 		cards.push(Ok(batches.iter().map(|x| {
// 			x.num_rows()
// 		}).sum()));
// 		println!("got {} and putting result into index {}",
// 				 time.as_millis(), times.len());
// 		times.push(Ok(time));
// 	} else {
// 		cards.push(Err(MeasureError::Timeout));
// 		times.push(Err(MeasureError::Timeout));
// 	}
// 	for child in node.children() {
// 		measure_subplan(child.clone(), ctx.clone(), cfg, cards, times).await?;
// 	}
// 	Ok(())
// }


/// Benchmark runner.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the plan to benchmark 
    #[arg(short, long)]
    plan_path: String,

	/// Path to benchmark config
	#[arg(short, long)]
    config_path: String,

	/// Path to place output
	#[arg(short, long)]
    output_path: String,

	/// Path to schema info 
	#[arg(short, long)]
    schemas_path: String,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();

	let cfg_bytes = std::fs::read(Path::new(&args.config_path))?;
	let cfg: BenchmarkConfig = serde_json::from_slice(&cfg_bytes)?;

	let schema_bytes = std::fs::read(Path::new(&args.schemas_path))?;
	let schemas: Vec<(String, Schema)> = serde_json::from_slice(&schema_bytes)?;
	
	let df_ctx = SessionContext::new();
	// TODO TODO this is hardcoded very sad :( 
	for tableref in schemas {
		let options = ParquetReadOptions::new().schema(&tableref.1);
		let path = format!("./tpch-data/{}.parquet", tableref.0);
		let table_path = std::path::Path::new(&path).canonicalize()?;
		df_ctx.register_parquet(
			tableref.0,
			table_path.to_str().unwrap(), options
		).await?;
	}	

	let plan_bytes = std::fs::read(Path::new(&args.plan_path))?;
	let plan = physical_plan_from_bytes(&plan_bytes, &df_ctx)?;
	let res = time_subplan(plan, df_ctx.task_ctx(), cfg.timeout).await?;
		
	let serialized = serde_json::to_string(&res)?;
	std::fs::write(&args.output_path, &serialized)?;
	
	Ok(())
}

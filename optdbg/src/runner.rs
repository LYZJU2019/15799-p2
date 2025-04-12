use std::path::Path;
/// Actual benchmarking script called by the benchmark process.
/// This is it's own target because we need process isolation: oftentimes bad workloads
/// are *so* bad that they will OOM and get the entire process killed (which sucks for us).

use std::sync::Arc;
use std::time::{Instant, Duration};

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::options::ReadOptions;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion::sql::TableReference;
use datafusion_proto::bytes::physical_plan_from_bytes;
use optdbg::benchmark::BenchmarkConfig;

use anyhow::Result;
use async_recursion::async_recursion;
use clap::Parser;
use datafusion::arrow::array::RecordBatch;
use datafusion::{execution::TaskContext, physical_plan::{collect, ExecutionPlan}};
use optdbg::common::PlanMeasurements;

async fn time_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	timeout: Option<Duration>,
) -> Result<Option<(Vec<RecordBatch>, Duration)>> {
	let (result_tx, result_rx) = tokio::sync::oneshot::channel();
	let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
	if let Some(timeout) = timeout {
		let (node, ctx) = (node.clone(), ctx.clone());
		let blocking_task = tokio::task::spawn(async move {
			let before = Instant::now();
			let out = collect(node, ctx).await;
			let after = Instant::now();
			let _ = result_tx.send((out, after-before));
		});
		tokio::spawn(async move {
			tokio::time::sleep(timeout).await;
			let _ = cancel_tx.send(());
		});
		tokio::select! {
			result = result_rx => {
				let (res, time) = result?;
				Ok(Some((res?, time)))
			}
			_ = cancel_rx => {
				Ok(None)
			}
		}
	} else {
		let (node, ctx) = (node.clone(), ctx.clone());
		let before = Instant::now();
		let out = collect(node, ctx).await;
		let after = Instant::now();
		Ok(Some((out?, after-before)))
	}
}

/// Recursively populate cardinality and runtime arrays.
// TODO need to be a *lot* more rigorous for the actual benchmarking here.
// one option is to try and integrate an existing optimizer like criterion
// or divan. Both of these don't really support usage as a library though...
// Doing this properly is an easy way to surpass TAQO.
//
// The other major TODO (this is long term) is to support the `fast` option 
// and implement the optimization in www.vldb.org/pvldb/vol2/vldb09-294.pdf
#[async_recursion]
async fn measure_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	cards: &mut Vec<Option<usize>>,
	times: &mut Vec<Option<Duration>>,
) -> Result<()> {
	if let Some((batches, time)) = time_subplan(node.clone(), ctx.clone(), cfg.timeout).await? {
		cards.push(Some(batches.iter().map(|x| x.num_rows()).sum()));
		times.push(Some(time));
	} else {
		cards.push(None);
		times.push(None);
	}
	for child in node.children() {
		measure_subplan(child.clone(), ctx.clone(), cfg, cards, times).await?;
	}
	Ok(())
}


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
	for tableref in schemas {
		let options = CsvReadOptions::new().delimiter(b'|').quote(b'"')
			.schema(&tableref.1);
		let path = format!("./tpch-data/{}.csv", tableref.0);
		let table_path = std::path::Path::new(&path).canonicalize()?;
		df_ctx.register_csv(tableref.0, table_path.to_str().unwrap(), options).await?;
	}	

	let plan_bytes = std::fs::read(Path::new(&args.plan_path))?;
	let plan = physical_plan_from_bytes(&plan_bytes, &df_ctx)?;
	let mut cards = Vec::new();
	let mut times = Vec::new();
	measure_subplan(plan, df_ctx.task_ctx(), &cfg, &mut cards, &mut times).await?;
	if let Some(time) = times[0] {
		println!("got time {}ms", time.as_millis());
	} else {
		println!("timed out!");
	}
	
	let measures = PlanMeasurements {
		cardinalities: cards,
		sub_runtimes: Some(times),
	};
	let serialized = serde_json::to_string(&measures)?;
	std::fs::write(&args.output_path, &serialized)?;
	
	// println!("{:?}", physical_round_trip);
	Ok(())
}

use std::sync::Arc;

use datafusion::{execution::TaskContext, physical_plan::{collect, ExecutionPlan}};
use anyhow::Result;
use async_recursion::async_recursion;

use crate::sampling::{Plan, SampleOutput};

// placeholder type
pub struct OptimizerMetrics {
	pub optimality: f32,
	pub efficiency: f32,
}

pub struct MeasuredPlan {
	pub plan: Plan,
	/// Time it took to run the plan.
	pub runtime: chrono::Duration,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<usize>,
	/// Preorder array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<chrono::Duration>>,
}

pub struct BenchmarkOutput {
	/// Sorted by runtime.
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
}

#[async_recursion]
async fn measure_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	cards: &mut Vec<usize>,
	times: &mut Vec<chrono::Duration>
) -> Result<()> {
	let before = chrono::Local::now();
	let batches = collect(node.clone(), ctx.clone()).await?;
	let after = chrono::Local::now();
	cards.push(batches.iter().map(|x| x.num_rows()).sum());
	times.push(after-before);
	for child in node.children() {
		measure_subplan(child.clone(), ctx.clone(), cards, times).await?;
	}
	Ok(())
}

async fn measure_plan(plan: Plan, ctx: Arc<TaskContext>) -> Result<MeasuredPlan> {
	let mut cardinalities = Vec::new();
	let mut runtimes = Vec::new();
	measure_subplan(plan.tree.clone(), ctx, &mut cardinalities, &mut runtimes).await?;
	Ok(MeasuredPlan {
		plan,
		runtime: runtimes[0],
		cardinalities,
		sub_runtimes: Some(runtimes),
	})
}

pub async fn benchmark(sample: SampleOutput) -> Result<BenchmarkOutput> {
	let ctx = sample.session.task_ctx();
	let mut out = vec![measure_plan(sample.best_plan, ctx.clone()).await?];
	for plan in sample.alternates {
		out.push(measure_plan(plan, ctx.clone()).await?);
	}
	Ok(BenchmarkOutput {
		plans: out,
		chosen_idx: todo!(),
		metrics: todo!(),
	})
}

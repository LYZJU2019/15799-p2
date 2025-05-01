use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanVisitor, PhysicalExpr};
use datafusion_proto::bytes::physical_plan_to_bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use async_recursion::async_recursion;
use child_wait_timeout::ChildWT;

use crate::sampling::SampleOutput;
use crate::common::{dump_plan, MeasureError, Plan, PlanMeasurements};

// Plan cache to avoid re-executing identical plans
#[derive(Default)]
struct PlanCache {
	// Maps plan hash to execution results (cardinality, duration)
	cache: HashMap<u64, Result<(usize, Duration), MeasureError>>,
	// Track cache hits for statistics
	hits: usize,
	// Track cache misses
	misses: usize,
}

impl PlanCache {
	fn new() -> Self {
		Self {
			cache: HashMap::new(),
			hits: 0,
			misses: 0,
		}
	}

	// Try to get a cached result
	fn get(&mut self, plan_hash: u64) -> Option<&Result<(usize, Duration), MeasureError>> {
		if let Some(result) = self.cache.get(&plan_hash) {
			self.hits += 1;
			Some(result)
		} else {
			self.misses += 1;
			None
		}
	}

	// Cache a new result
	fn insert(&mut self, plan_hash: u64, result: Result<(usize, Duration), MeasureError>) {
		self.cache.insert(plan_hash, result);
	}

	// Get cache statistics
	fn stats(&self) -> (usize, usize, f64) {
		let total = self.hits + self.misses;
		let hit_rate = if total > 0 {
			self.hits as f64 / total as f64
		} else {
			0.0
		};
		(self.hits, self.misses, hit_rate)
	}
}

// Thread-local storage for the plan cache
thread_local! {
	static PLAN_CACHE: std::cell::RefCell<PlanCache> = std::cell::RefCell::new(PlanCache::new());
}

// Hash function for execution plans
fn hash_plan(plan: &Arc<dyn ExecutionPlan>) -> u64 {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};
	
	// Create a hasher
	let mut hasher = DefaultHasher::new();
	
	// Hash the plan type and schema
	plan.name().hash(&mut hasher);
	plan.schema().to_string().hash(&mut hasher);
	
	// Manually visit each child and hash it too
	fn visit_plan(plan: &Arc<dyn ExecutionPlan>, hasher: &mut DefaultHasher) {
		plan.name().hash(hasher);
		plan.schema().to_string().hash(hasher);
		
		// Visit children
		for child in plan.children() {
			visit_plan(&child, hasher);
		}
	}
	
	// Start recursion
	visit_plan(plan, &mut hasher);
	
	// Return the hash
	hasher.finish()
}

// Determine if a plan is small enough to cache
fn is_plan_cacheable(plan: &Arc<dyn ExecutionPlan>) -> bool {
	// Only cache plans with limited number of children to save memory
	// This is a simple heuristic - you may want to use more sophisticated criteria
	let max_nodes = 10;
	let mut node_count = 0;
	
	fn count_nodes(plan: &Arc<dyn ExecutionPlan>, count: &mut usize) {
		*count += 1;
		for child in plan.children() {
			count_nodes(&child, count);
		}
	}
	
	count_nodes(plan, &mut node_count);
	node_count <= max_nodes
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct BenchmarkConfig {
	pub fast: bool,
	pub timeout: Option<Duration>,
	pub enable_cache: bool,
}

// placeholder type
pub struct OptimizerMetrics {
	/// Raw TAQO score 's' (lower is better)
	pub taqo_score_s: f64,
	/// TAQO accuracy converted to percentage (0-100, higher is better)
	pub taqo_accuracy_percent: f64,
	/// Performance Factor (PF) - percentage of plans that perform worse than or equal to the optimizer-chosen plan
	pub performance_factor: f64,
	/// Average Q-Error for cardinality estimation (closer to 1.0 is better)
	pub avg_q_error: f64,
}

pub struct BenchmarkOutput {
	pub name: String,
	/// Sorted by runtime (fastest at front).
	pub plans: Vec<MeasuredPlan>,
	/// Index of the chosen plan in the `plans` field.
	pub chosen_idx: usize,
	/// Global optimizer metrics.
	pub metrics: OptimizerMetrics,
}

/// Plan annotated with runtimes / cardinalities.
pub struct MeasuredPlan {
	pub plan: Plan,
	/// Time it took to run the overall plan. None if timeout hit.
	pub runtime: Result<Duration, MeasureError>,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<Result<usize, MeasureError>>,
	/// Preorder array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<Result<Duration, MeasureError>>>,
}


impl std::fmt::Display for MeasuredPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let times = self.sub_runtimes
			.as_ref()
			.unwrap()
			.iter()
			.map(|x| x.map(|x| x.as_millis()).unwrap_or(9999999)).collect();
        let mut i = 0;
        crate::common::format_plan_with_2preorder_help(
            f,
            self.plan.tree.clone(),
            0,
            &times,
            &self.plan.est_costs,
            &mut i,
        )
    }
}

impl MeasuredPlan {
	fn new(
		plan: Plan,
		measurements: PlanMeasurements
	) -> Self {
		Self {
			plan,
			// FIXME need to change this when implementing covering queries!
			runtime: measurements.sub_runtimes.as_ref().unwrap()[0].clone(),
			cardinalities: measurements.cardinalities,
			sub_runtimes: measurements.sub_runtimes
		}
	}
}

// /// Times a subplan's execution, giving up after a timeout.
// async fn time_subplan(
// 	node: Arc<dyn ExecutionPlan>,
// 	ctx: Arc<TaskContext>,
// 	timeout: Option<Duration>,
// ) -> anyhow::Result<Option<(Vec<RecordBatch>, Duration)>> {
// 	// Ensure a timeout is set
// 	let timeout = timeout.unwrap_or(Duration::from_secs(5));
// 	// Calculate hard timeout (1.5 times the original timeout)
// 	let hard_timeout = timeout.checked_add(timeout.checked_div(2).unwrap_or(Duration::from_millis(500))).unwrap_or(Duration::from_secs(10));
	
// 	// Start timing
// 	let start = Instant::now();
	
// 	// Create two channels for communication
// 	let (result_tx, result_rx) = tokio::sync::oneshot::channel();
// 	let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
	
// 	// Start execution task
// 	let handle = tokio::spawn(async move {
// 		// Catch potential panics
// 		let result = match tokio::task::spawn(async {
// 			collect(node, ctx).await
// 		}).await {
// 			Ok(res) => res,
// 			Err(e) => {
// 				println!("Worker thread panicked: {}", e);
// 				Err(datafusion::error::DataFusionError::Execution("Panic occurred during query execution".to_string()))
// 			}
// 		};
		
// 		// Send result (success or error)
// 		let elapsed = start.elapsed();
// 		let _ = result_tx.send((result, elapsed));
// 	});
	
// 	// Start timeout task
// 	tokio::spawn(async move {
// 		tokio::time::sleep(timeout).await;
// 		let _ = cancel_tx.send(());
// 	});
	
// 	// Wait for result or timeout
// 	tokio::select! {
// 		result = result_rx => {
// 			match result {
// 				Ok((res, time)) => {
// 					println!("Query completed in {:?}", time);
// 					match res {
// 						Ok(data) => Ok(Some((data, time))),
// 						Err(e) => {
// 							println!("Error during execution: {}", e);
// 							Ok(None)
// 						}
// 					}
// 				},
// 				Err(e) => {
// 					println!("Channel closed unexpectedly: {}", e);
// 					Ok(None)
// 				}
// 			}
// 		},
// 		_ = cancel_rx => {
// 			println!("Timeout reached, cancelling task");
// 			handle.abort();
// 			Ok(None)
// 		},
// 		// Hard timeout to ensure no indefinite waiting
// 		_ = tokio::time::sleep(hard_timeout) => {
// 			println!("Hard timeout reached");
// 			handle.abort();
// 			Ok(None)
// 		}
// 	}
// }

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
// 	// First execute the current node
// 	if let Some((batches, time)) = time_subplan(node.clone(), ctx.clone(), cfg.timeout).await? {
// 		println!("\n=== Plan Execution Details ===");
// 		println!("Number of batches received: {}", batches.len());
// 		for (i, batch) in batches.iter().enumerate() {
// 			println!("Batch {}: {} rows × {} columns", i, batch.num_rows(), batch.num_columns());
// 		}
// 		let cardinality: usize = batches.iter().map(|x| x.num_rows()).sum();
// 		println!("Total cardinality: {}", cardinality);
// 		println!("Execution time: {:?}", time);
// 		println!("============================\n");
		
// 		// Push current node's result
// 		cards.push(Ok(cardinality));
// 		times.push(Ok(time));
		
// 		// If current node executed successfully, then process its children
// 		for i in 0..node.children().len() {
// 			let child = node.children()[i].clone();
// 			measure_subplan(child, ctx.clone(), cfg, cards, times).await?;
// 		}
// 	} else {
// 		// Current node timed out, no need to process children
// 		cards.push(Err(MeasureError::Timeout));
// 		times.push(Err(MeasureError::Timeout));
		
// 		// Use a non-recursive approach to mark all descendants as errors
// 		// First, add all immediate children to our stack
// 		let mut stack = Vec::new();
// 		for i in 0..node.children().len() {
// 			stack.push(node.children()[i].clone());
// 		}
		
// 		// Process the stack until empty
// 		while let Some(child_node) = stack.pop() {
// 			// Mark this node as timeout error
// 			cards.push(Err(MeasureError::Timeout));
// 			times.push(Err(MeasureError::Timeout));
			
// 			// Add its children to the stack
// 			for i in 0..child_node.children().len() {
// 				stack.push(child_node.children()[i].clone());
// 			}
// 		}
// 	}
// 	Ok(())
// }


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
	est_costs: &Vec<f64>,
	idx: &mut usize,
	cards: &mut Vec<Result<usize, MeasureError>>,
	times: &mut Vec<Result<Duration, MeasureError>>,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<()> {
	// FIXME temporary hack
	println!("est cost is {}", est_costs[*idx]);
	// if est_costs[*idx] > 1000000000000.0 || node.name() == "CrossJoinExec" {
	// 	cards.push(Err(MeasureError::Died));
	// 	times.push(Err(MeasureError::Died));
	// } else { 
		println!("Timing subplan");
		crate::common::dump_plan(node.clone(), 0);	
		match time_subplan_ipc(node.clone(), cfg, tables.clone()).await? {
			Ok((card, time)) => {
				cards.push(Ok(card));
				println!("got {} and putting result into index {}",
						 time.as_millis(), times.len());
				times.push(Ok(time));
			},
			Err(e) => {
				cards.push(Err(e));
				times.push(Err(e));
			}
		}
	// }
	for child in node.children() {
		*idx += 1;
		measure_subplan(child.clone(), ctx.clone(), cfg, &est_costs,
						idx, cards, times, tables.clone()).await?;
	}
	Ok(())
}

/// Measure cardinalities and runtimes of plan and subplans.
///
/// Use `measure_ipc` for OOM safety (which is a real concern).
// TODO think of a more consistent hack? do we bother keeping this at all?
async fn measure_plan(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<MeasuredPlan> {
	let mut cardinalities = Vec::new();
	let mut runtimes = Vec::new();
	// FIXME temporary hack to get around OOMs. obviously not generic.
	dump_plan(plan.tree.clone(), 0);
	println!("{:?}", plan.est_costs);
	let mut idx = 0;
	measure_subplan(plan.tree.clone(), ctx, cfg, &plan.est_costs, &mut idx,
					&mut cardinalities, &mut runtimes, tables).await?;
	if let Ok(runtime) = runtimes[0] {
		println!("ran plan in {}ms (est cost {})", runtime.as_millis(), plan.est_costs[0]);
	} else {
		println!("plan timed out (est cost {})", plan.est_costs[0]);
	}
	Ok(MeasuredPlan {
		plan,
		runtime: runtimes[0],
		cardinalities,
		sub_runtimes: Some(runtimes),
	})
}

/// Measure cardinalities and runtimes of plan and subplans.
async fn time_subplan_ipc(
	plan: Arc<dyn ExecutionPlan>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<Result<(usize, Duration), MeasureError>> {
	// Skip cache if caching is disabled in config
	if !cfg.enable_cache {
		return run_plan_ipc(plan, cfg, tables).await;
	}
	
	// Only cache if plan is small enough
	if !is_plan_cacheable(&plan) {
		return run_plan_ipc(plan, cfg, tables).await;
	}
	
	// Calculate plan hash
	let plan_hash = hash_plan(&plan);
	
	// Check cache
	let cached_result = PLAN_CACHE.with(|cache| {
		cache.borrow_mut().get(plan_hash).cloned()
	});
	
	if let Some(result) = cached_result {
		println!("Cache hit! Reusing previous execution result");
		return Ok(result);
	}
	
	// Cache miss, execute the plan
	let result = run_plan_ipc(plan, cfg, tables).await?;
	
	// Update cache with new result
	PLAN_CACHE.with(|cache| {
		cache.borrow_mut().insert(plan_hash, result.clone());
	});
	
	// Periodically log cache statistics
	PLAN_CACHE.with(|cache| {
		let (hits, misses, rate) = cache.borrow().stats();
		if (hits + misses) % 100 == 0 && hits + misses > 0 {
			println!("Plan cache: {} hits, {} misses, {:.2}% hit rate", 
					 hits, misses, rate * 100.0);
		}
	});
	
	Ok(result)
}

/// Actual implementation that runs the plan through IPC
async fn run_plan_ipc(
	plan: Arc<dyn ExecutionPlan>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<Result<(usize, Duration), MeasureError>> {
	let bytes = physical_plan_to_bytes(plan)?;
	let mut plan_file = tempfile::NamedTempFile::new()?;
	plan_file.write_all(&bytes)?;

	let mut schemas = Vec::new();
	for i in tables.table_names() {
		schemas.push((i.clone(), tables.table(&i).await?.unwrap().schema()));
	}
	
	let bytes = serde_json::to_string(&schemas)?;
	let mut schema_file = tempfile::NamedTempFile::new()?;
	schema_file.write_all(bytes.as_bytes())?;
	
	let out_file = tempfile::NamedTempFile::new()?;

	let mut child = std::process::Command::new("../optdbg/target/release/runner")
		.arg("-p").arg(plan_file.path())
		.arg("-s").arg(schema_file.path())
		.arg("-o").arg(out_file.path()).spawn()?;

	let status = if let Some(timeout) = cfg.timeout {
		child.wait_timeout(timeout)
	} else { child.wait() };

	match status {
		Ok(status) => {
			if !status.success() {
				Ok(Err(MeasureError::Died))
			} else {
				let res: (usize, Duration) = serde_json::from_reader(out_file)?;
				Ok(Ok(res))
			}
		},
		Err(e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(Err(MeasureError::Timeout)),
		Err(e) => Err(anyhow::anyhow!("subprocess error: {e}"))
	}
}

/// Calculates the accuracy of cost estimation using TAQO's weighted Kendall's Tau coefficient
/// Returns the raw score 's' and a derived percentage accuracy (0-100, higher is better).
fn calculate_cost_rank_accuracy(plans: &[MeasuredPlan]) -> (f64, f64) {
	// Create a vector of (actual_runtime, estimated_cost) pairs
	let mut runtime_cost_pairs: Vec<(Duration, f64)> = Vec::new();
	
	// Print detailed plan information
	println!("\nDetailed plan information:");
	println!("{:<8} {:<15} {:<20}", 
		"Plan #", "Runtime (ms)", "Estimated Cost");
	println!("{:-<45}", "");
	
	for (i, plan) in plans.iter().enumerate() {
		let runtime = match plan.runtime {
			Ok(duration) => format!("{:.2}", duration.as_secs_f64() * 1000.0),
			Err(_) => "Error".to_string(),
		};
		
		println!("{:<8} {:<15} {:<20.2}", 
			i, runtime, plan.plan.est_costs[0]);
	}
	println!("{:-<45}\n", "");
	
	// Collect valid measurements
	for plan in plans {
		if let Ok(runtime) = plan.runtime {
			runtime_cost_pairs.push((runtime, plan.plan.est_costs[0]));
		}
	}
	
	// Find the best actual runtime for weight calculation (a1 in the paper)
	let best_runtime = runtime_cost_pairs.iter()
		.map(|(r, _)| *r)
		.min()
		.unwrap();
	
	// Find min and max values for normalization (a_n, a_1, max(e_k), min(e_k))
	let min_r = best_runtime.as_secs_f64(); // a1
	let max_r = runtime_cost_pairs.iter()
		.map(|(r, _)| r.as_secs_f64())
		.max_by(|a, b| a.partial_cmp(b).unwrap()) // a_n
		.unwrap();
	let min_e = runtime_cost_pairs.iter()
		.map(|(_, e)| *e)
		.min_by(|a, b| a.partial_cmp(b).unwrap())
		.unwrap();
	let max_e = runtime_cost_pairs.iter()
		.map(|(_, e)| *e)
		.max_by(|a, b| a.partial_cmp(b).unwrap())
		.unwrap();
	
	// Avoid division by zero if all runtimes or costs are identical
	let range_r = max_r - min_r;
	let range_e = max_e - min_e;
	
	// Calculate weighted Kendall's Tau score 's' (Equation 4)
	let mut s = 0.0;
	let n = runtime_cost_pairs.len();
	
	for i in 0..n {
		for j in (i+1)..n {
			let (r_i_dur, e_i) = runtime_cost_pairs[i];
			let (r_j_dur, e_j) = runtime_cost_pairs[j];
			
			// Skip pairs with identical estimated costs (sgn(0) is undefined/ignored)
			if e_i == e_j {
				continue;
			}

			let r_i = r_i_dur.as_secs_f64(); // a_i
			let r_j = r_j_dur.as_secs_f64(); // a_j
			
			// Calculate weights (Equation 2, w_m = a1 / am)
			let w_i = if r_i > 0.0 { min_r / r_i } else { 0.0 }; 
			let w_j = if r_j > 0.0 { min_r / r_j } else { 0.0 };
			
			// Calculate normalized Euclidean distance (Equation 3)
			let term1 = if range_r > 0.0 { ((r_j - r_i) / range_r).powi(2) } else { 0.0 };
			let term2 = if range_e > 0.0 { ((e_j - e_i) / range_e).powi(2) } else { 0.0 };
			let d_ij = (term1 + term2).sqrt();
			
			// Calculate sign based *only* on estimated costs (sgn(ej - ei))
			let sign = (e_j - e_i).signum(); // Returns 1.0 or -1.0 since e_i != e_j
			
			let term = w_i * w_j * d_ij;
			
			// Add to sum (Equation 4)
			s += term * sign;
		}
	}
	
	// Calculate percentage accuracy (0-100, higher is better)
	let accuracy_percent = 100.0 * (-s.abs()).exp();
	
	// Return the raw score 's' and the percentage accuracy
	(s, accuracy_percent)
}

/// Calculates Q-Error between estimated and actual cardinality
/// Q-Error is defined as max(est/act, act/est) and is always >= 1.0
/// A Q-Error of 1.0 means perfect estimation
pub fn calculate_q_error(estimated: f64, actual: usize) -> f64 {
	if estimated == 0.0 && actual == 0 {
		return 1.0; // Perfect estimation
	}
	if estimated == 0.0 || actual == 0 {
		return f64::MAX; // Infinite error
	}
	
	// Calculate Q-Error as max(est/act, act/est)
	f64::max(
		estimated / actual as f64,
		actual as f64 / estimated
	)
}

/// Calculates average Q-Error for a plan's cardinality estimations
/// Returns a value >= 1.0, where 1.0 means perfect estimation
fn calculate_avg_q_error(plan: &MeasuredPlan) -> f64 {
	let mut total_q_error = 0.0;
	let mut valid_counts = 0;
	
	for (i, cardinality_result) in plan.cardinalities.iter().enumerate() {
		if i < plan.plan.est_cards.len() {
			if let Ok(actual_card) = cardinality_result {
				// Skip if actual cardinality is 0
				if *actual_card == 0 {
					println!("Node {}: Skipping - actual cardinality is 0", i);
					continue;
				}
				
				let estimated_card = plan.plan.est_cards[i];
				let q_error = calculate_q_error(estimated_card, *actual_card);
				if q_error.is_finite() {
					total_q_error += q_error;
					valid_counts += 1;
				}
			}
		}
	}
	
	if valid_counts == 0 {
		return f64::MAX;
	}
	
	total_q_error / valid_counts as f64
}

/// Calculates the Performance Factor (PF) - the proportion of plans that perform
/// worse than or equal to the optimizer-chosen plan.
/// 
/// PF = 1 indicates the optimizer chose the best possible plan.
fn calculate_performance_factor(plans: &[MeasuredPlan], chosen_idx: usize) -> f64 {
	// Get the runtime of the chosen plan
	let chosen_runtime = match &plans[chosen_idx].runtime {
		Ok(duration) => Some(*duration),
		Err(_) => None, // If the chosen plan failed, it can't be compared
	};
	
	if chosen_runtime.is_none() {
		return 0.0;
	}
	
	// Count the number of plans that perform worse than or equal to the chosen plan
	let chosen_runtime = chosen_runtime.unwrap();
	let mut plans_worse_or_equal = 0;
	let mut valid_plans = 0;
	
	for plan in plans {
		if let Ok(runtime) = plan.runtime {
			valid_plans += 1;
			if runtime >= chosen_runtime {
				plans_worse_or_equal += 1;
			}
		}
	}
	
	if valid_plans == 0 {
		return 0.0;
	}
	
	// Return PF as a fraction (0.0 to 1.0)
	plans_worse_or_equal as f64 / valid_plans as f64
}

#[derive(PartialEq, Eq)]
pub enum CardQuality {
	Excellent,
	Good,
	Acceptable,
	Poor,
	Unknown
}

impl CardQuality {
	pub fn from_q_err(q: f64) -> Self {
		if !q.is_finite() {
			Self::Unknown
		} else if q <= 2.0 {
			Self::Excellent
		} else if q <= 4.0 {
			Self::Good
		} else if q <= 10.0 {
			Self::Acceptable
		} else {
			Self::Poor
		} 
	}
}

pub fn benchmark(
	samples: impl Stream<Item = SampleOutput>,
	cfg: BenchmarkConfig
) -> impl Stream<Item = BenchmarkOutput> {
	samples.then(move |sample| async move {
		println!("{} {}", sample.name, sample.alternates.len());
		let ctx = sample.session.task_ctx();
		let mut out = Vec::new();
		// TODO best measurement should definitely be interleaved in to avoid
		// warmup time affecting measurements or something like that...
		let best = measure_plan(sample.best_plan, ctx.clone(), &cfg, sample.tables.clone()).await?;
		for plan in sample.alternates.into_iter()
			.sorted_by(|x, y| x.est_costs[0].partial_cmp(&y.est_costs[0]).unwrap()) {
				out.push(measure_plan(plan, ctx.clone(), &cfg, sample.tables.clone()).await?);
			}
		out.sort_by(|x, y| {
			if x.runtime.is_err() {
				std::cmp::Ordering::Greater
			} else if y.runtime.is_err() {
				std::cmp::Ordering::Less
			} else {
				x.runtime.unwrap().cmp(&y.runtime.unwrap())
			}
		});
		let chosen_idx = out.iter()
			.position(|x| x.runtime.is_err() ||
					  best.runtime.is_ok() && x.runtime.unwrap() > best.runtime.unwrap())
			.unwrap_or(out.len());
		out.insert(chosen_idx, best);
		
		// Calculate cost rank accuracy
		let (taqo_s, taqo_percent) = calculate_cost_rank_accuracy(&out);
		
		// Calculate Performance Factor (PF)
		let performance_factor = calculate_performance_factor(&out, chosen_idx);
		
		// Calculate average Q-Error for the chosen plan
		let avg_q_error = calculate_avg_q_error(&out[chosen_idx]);
		
		Ok(BenchmarkOutput {
			name: sample.name,
			plans: out,
			chosen_idx,
			metrics: OptimizerMetrics {
				taqo_score_s: taqo_s,
				taqo_accuracy_percent: taqo_percent,
				performance_factor,
				avg_q_error,
			}
		})
	}).filter_map(|x: anyhow::Result<BenchmarkOutput>| async {
		x.ok()
	})
}

/// Calculates the Optimality Frequency (OF) across a workload of queries.
/// 
/// OF is the fraction of queries for which the optimizer chooses the relative optimal plan (PF = 1).
/// 
/// Returns a value between 0.0 and 1.0, where 1.0 means the optimizer chose the best plan for all queries.
pub fn calculate_optimality_frequency(benchmark_results: &[BenchmarkOutput]) -> f64 {
	if benchmark_results.is_empty() {
		return 0.0;
	}
	
	// Count queries where PF = 1 (optimizer chose the best plan)
	let optimal_queries = benchmark_results.iter()
		.filter(|result| {
			// Check if PF is 1.0 (or very close to 1.0 due to floating-point precision)
			(result.metrics.performance_factor - 1.0).abs() < 1e-6
		})
		.count();
	
	// Print OF analysis
	println!("\nOptimality Frequency (OF) Analysis:");
	println!("Queries with optimal plan selection: {}/{}", optimal_queries, benchmark_results.len());
	println!("Optimality Frequency: {:.2}%", (optimal_queries as f64 / benchmark_results.len() as f64) * 100.0);
	
	// Return OF as a fraction (0.0 to 1.0)
	optimal_queries as f64 / benchmark_results.len() as f64
}

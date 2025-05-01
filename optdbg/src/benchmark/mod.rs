use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion_proto::bytes::physical_plan_to_bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use async_recursion::async_recursion;

use crate::sampling::SampleOutput;
use crate::common::{Plan, PlanMeasurements, MeasureError};

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct BenchmarkConfig {
	pub fast: bool,
	pub timeout: Option<Duration>,
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
	// /// Optimality Frequency (OF) - fraction of queries for which the optimizer chooses the relative optimal plan
	// pub optimality_frequency: f64,
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

// this version is for cross-process stuff...
// but switching to `ListingTable`s kinda fixed OOM issues
/// Measure cardinalities and runtimes of plan and subplans.
async fn measure_plan_ipc(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<MeasuredPlan> {
	let bytes = physical_plan_to_bytes(plan.clone().tree)?;
	let mut plan_file = tempfile::NamedTempFile::new()?;
	plan_file.write_all(&bytes)?;

	let bytes = serde_json::to_string(cfg)?;
	let mut cfg_file = tempfile::NamedTempFile::new()?;
	cfg_file.write_all(bytes.as_bytes())?;

	let mut schemas = Vec::new();
	for i in tables.table_names() {
		schemas.push((i.clone(), tables.table(&i).await?.unwrap().schema()));
	}
	
	let bytes = serde_json::to_string(&schemas)?;
	let mut schema_file = tempfile::NamedTempFile::new()?;
	schema_file.write_all(bytes.as_bytes())?;

	let out_file = tempfile::NamedTempFile::new()?;
	let output = std::process::Command::new("../optdbg/target/release/runner")
		.arg("-p").arg(plan_file.path())
		.arg("-c").arg(cfg_file.path())
		.arg("-s").arg(schema_file.path())
		.arg("-o").arg(out_file.path())
		.status()?;
	
	if !output.success() {
		let size = plan.size();
		Ok(MeasuredPlan {
			plan,
			runtime: Err(MeasureError::Died),
			cardinalities: vec![Err(MeasureError::Died); size],
			sub_runtimes: Some(vec![Err(MeasureError::Died); size]),
		})
	} else {
		let measurements: PlanMeasurements = serde_json::from_reader(out_file)?;
		Ok(MeasuredPlan::new(plan, measurements))
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
		.min();
	
	// If all plans failed (no valid runtimes), return default values
	if best_runtime.is_none() {
		println!("Warning: All plans failed execution, returning default metrics");
		return (0.0, 0.0); // Return default values indicating no valid metrics
	}
	
	let best_runtime = best_runtime.unwrap();
	
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
	
	println!("\nPerformance Factor (PF): {:.2}%", (plans_worse_or_equal as f64 / valid_plans as f64) * 100.0);
	
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
		let ctx = sample.session.task_ctx();
		let mut out = Vec::new();
		// TODO best measurement should definitely be interleaved in to avoid
		// warmup time affecting measurements or something like that...
		let best = measure_plan_ipc(sample.best_plan, ctx.clone(), &cfg, sample.tables.clone()).await?;
		for plan in sample.alternates.into_iter()
			.sorted_by(|x, y| x.est_costs[0].partial_cmp(&y.est_costs[0]).unwrap()) {
				out.push(measure_plan_ipc(plan, ctx.clone(), &cfg, sample.tables.clone()).await?);
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

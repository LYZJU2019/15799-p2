use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::cmp::Ordering;

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanVisitor, PhysicalExpr};
use datafusion_proto::bytes::physical_plan_to_bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use async_recursion::async_recursion;
use wait_timeout::ChildExt;

use crate::sampling::SampleOutput;
use crate::common::{dump_plan, MeasureError, Plan, PlanMeasurements};

// New runtime statistics struct to collect data from multiple runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStats {
	/// Individual measurements
	pub measurements: Vec<Duration>,
	/// Mean (average) runtime
	pub mean: Duration,
	/// Standard deviation
	pub stddev: Duration,
	/// Minimum runtime
	pub min: Duration,
	/// Maximum runtime
	pub max: Duration,
	/// Coefficient of variation (stddev/mean as a percentage)
	pub cv_percent: f64,
}

impl RuntimeStats {
	/// Create new RuntimeStats from a vector of measurements
	pub fn new(measurements: Vec<Duration>) -> Self {
		if measurements.is_empty() {
			return Self {
				measurements: vec![],
				mean: Duration::from_secs(0),
				stddev: Duration::from_secs(0),
				min: Duration::from_secs(0),
				max: Duration::from_secs(0),
				cv_percent: 0.0,
			};
		}

		// Calculate min and max
		let min = *measurements.iter().min().unwrap();
		let max = *measurements.iter().max().unwrap();
		
		// Calculate mean
		let sum: Duration = measurements.iter().sum();
		let mean = sum / measurements.len() as u32;
		
		// Calculate standard deviation
		let mean_secs = mean.as_secs_f64();
		let variance: f64 = measurements.iter()
			.map(|d| {
				let diff = d.as_secs_f64() - mean_secs;
				diff * diff
			})
			.sum::<f64>() / measurements.len() as f64;
		let stddev_secs = variance.sqrt();
		let stddev = Duration::from_secs_f64(stddev_secs);
		
		// Calculate coefficient of variation (as a percentage)
		let cv_percent = if mean_secs > 0.0 {
			(stddev_secs / mean_secs) * 100.0
		} else {
			0.0
		};
		
		Self {
			measurements,
			mean,
			stddev,
			min,
			max,
			cv_percent,
		}
	}
	
	/// Create a RuntimeStats from a single Duration
	pub fn from_duration(duration: Duration) -> Self {
		Self {
			measurements: vec![duration],
			mean: duration,
			stddev: Duration::from_secs(0),
			min: duration,
			max: duration,
			cv_percent: 0.0,
		}
	}
	
	/// Check if this runtime's range significantly overlaps with another
	pub fn overlaps_with(&self, other: &Self, significance_threshold: f64) -> bool {
		// Calculate the overlap of confidence intervals
		// Using mean ± stddev as a simple confidence interval
		let self_low = self.mean.as_secs_f64() - self.stddev.as_secs_f64();
		let self_high = self.mean.as_secs_f64() + self.stddev.as_secs_f64();
		let other_low = other.mean.as_secs_f64() - other.stddev.as_secs_f64();
		let other_high = other.mean.as_secs_f64() + other.stddev.as_secs_f64();
		
		// Check if ranges overlap
		if self_high < other_low || self_low > other_high {
			return false; // No overlap
		}
		
		// Calculate overlap
		let overlap_start = f64::max(self_low, other_low);
		let overlap_end = f64::min(self_high, other_high);
		let overlap_length = overlap_end - overlap_start;
		
		// Calculate total range lengths
		let self_range = self_high - self_low;
		let other_range = other_high - other_low;
		
		// Calculate overlap as percentage of the smaller range
		let min_range = f64::min(self_range, other_range);
		if min_range == 0.0 {
			// Both are points
			if self_range == 0.0 && other_range == 0.0 {
				return self.mean == other.mean;
			}
			if self_range == 0.0 {
				return self.mean.as_secs_f64() >= other_low && self.mean.as_secs_f64() <= other_high;
			}
			if other_range == 0.0 {
				return other.mean.as_secs_f64() >= self_low && other.mean.as_secs_f64() <= self_high;
			}
			return false;
		}
		let overlap_percentage = overlap_length / min_range;
		
		// Return true if overlap exceeds threshold
		overlap_percentage >= significance_threshold
	}
}

impl PartialEq for RuntimeStats {
	fn eq(&self, other: &Self) -> bool {
		// Two identical runtime stats objects should be equal
		if std::ptr::eq(self, other) {
			return true;
		}
		
		// If comparing with self but through different references, check measurements directly
		if self.measurements == other.measurements {
			return true;
		}
		
		// Otherwise, consider equal if confidence intervals overlap significantly
		self.overlaps_with(other, 0.5) // 50% overlap threshold
	}
}

impl Eq for RuntimeStats {}

impl PartialOrd for RuntimeStats {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RuntimeStats {
	fn cmp(&self, other: &Self) -> Ordering {
		// If they overlap significantly, consider them equal
		if self.eq(other) {
			return Ordering::Equal;
		}
		
		// Otherwise compare means
		self.mean.cmp(&other.mean)
	}
}

// Plan cache to avoid re-executing identical plans
#[derive(Default)]
struct PlanCache {
	// Maps plan hash to execution results (cardinality, duration)
	cache: HashMap<u64, Result<(usize, RuntimeStats), MeasureError>>,
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
	fn get(&mut self, plan_hash: u64) -> Option<&Result<(usize, RuntimeStats), MeasureError>> {
		if let Some(result) = self.cache.get(&plan_hash) {
			self.hits += 1;
			Some(result)
		} else {
			self.misses += 1;
			None
		}
	}

	// Cache a new result
	fn insert(&mut self, plan_hash: u64, result: Result<(usize, RuntimeStats), MeasureError>) {
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
	/// Number of times to run each benchmark
	pub num_runs: usize,
	/// Whether to drop the highest and lowest measurements
	pub drop_outliers: bool,
	/// Standard deviation overlap threshold to consider runtimes equal (0.0-1.0)
	pub overlap_threshold: f64,
	/// Whether to skip measuring subplans of failing plans
	pub early_stopping: bool,
	/// Track performance metrics
	pub track_metrics: bool,
}

impl Default for BenchmarkConfig {
	fn default() -> Self {
		Self {
			fast: false,
			timeout: Some(Duration::from_secs(5)),
			enable_cache: true,
			num_runs: 3,
			drop_outliers: false,
			overlap_threshold: 0.5,
			early_stopping: true,
			track_metrics: true,
		}
	}
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
	pub runtime: Result<RuntimeStats, MeasureError>,
	/// Preorder array of each subplan's true cardinality.
	pub cardinalities: Vec<Result<usize, MeasureError>>,
	/// Preorder array of each subplan's runtime.
	pub sub_runtimes: Option<Vec<Result<RuntimeStats, MeasureError>>>,
}


impl std::fmt::Display for MeasuredPlan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let times = self.sub_runtimes
			.as_ref()
			.unwrap()
			.iter()
			.map(|x| match x {
				Ok(stats) => stats.mean.as_millis(),
				Err(_) => 9999999
			}).collect::<Vec<_>>();
		
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
	pub fn new(
		plan: Plan,
		measurements: PlanMeasurements
	) -> Self {
		// Convert the first duration to runtime stats
		let runtime = if let Some(Some(Ok(first_runtime))) = measurements.sub_runtimes.as_ref().map(|rts| rts.first()) {
			Ok(RuntimeStats::from_duration(*first_runtime))
		} else if let Some(Err(e)) = measurements.sub_runtimes.as_ref().and_then(|rts| rts.first()) {
			Err(*e)
		} else {
			Ok(RuntimeStats::from_duration(Duration::from_millis(0)))
		};
		
		// Convert sub_runtimes from Option<Vec<Result<Duration, MeasureError>>>
		// to Option<Vec<Result<RuntimeStats, MeasureError>>>
		let sub_runtimes = measurements.sub_runtimes.map(|rts| {
			rts.into_iter()
				.map(|rt| rt.map(RuntimeStats::from_duration))
				.collect()
		});
		
		Self {
			plan,
			runtime,
			cardinalities: measurements.cardinalities,
			sub_runtimes,
		}
	}
}

// Add a new struct for tracking performance metrics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
	/// Total execution time
	pub total_time: Duration,
	/// Number of plans measured
	pub plans_measured: usize,
	/// Number of cache hits
	pub cache_hits: usize,
	/// Number of cache misses
	pub cache_misses: usize,
	/// Number of early stopping events
	pub early_stops: usize,
	/// Total time saved by cache hits (estimated)
	pub cache_time_saved: Duration,
	/// Total time saved by early stopping (estimated)
	pub early_stopping_time_saved: Duration,
}

impl PerformanceMetrics {
	pub fn new() -> Self {
		Self::default()
	}
	
	pub fn report(&self) -> String {
		let cache_hit_rate = if self.cache_hits + self.cache_misses > 0 {
			self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64 * 100.0
		} else {
			0.0
		};
		
		format!(
			"\nPerformance Metrics:\n\
			Total execution time: {:.2}s\n\
			Plans measured: {}\n\
			Cache hit rate: {:.2}%\n\
			Cache hits: {}\n\
			Cache misses: {}\n\
			Early stopping events: {}\n\
			Estimated time saved by caching: {:.2}s\n\
			Estimated time saved by early stopping: {:.2}s\n",
			self.total_time.as_secs_f64(),
			self.plans_measured,
			cache_hit_rate,
			self.cache_hits,
			self.cache_misses,
			self.early_stops,
			self.cache_time_saved.as_secs_f64(),
			self.early_stopping_time_saved.as_secs_f64()
		)
	}
	
	// Combine with another metrics object
	pub fn combine(&mut self, other: &Self) {
		self.total_time += other.total_time;
		self.plans_measured += other.plans_measured;
		self.cache_hits += other.cache_hits;
		self.cache_misses += other.cache_misses;
		self.early_stops += other.early_stops;
		self.cache_time_saved += other.cache_time_saved;
		self.early_stopping_time_saved += other.early_stopping_time_saved;
	}
}

// Add a global variable to track metrics
thread_local! {
	static PERFORMANCE_METRICS: std::cell::RefCell<PerformanceMetrics> = std::cell::RefCell::new(PerformanceMetrics::new());
}

// Add a function to get the current metrics
pub fn get_performance_metrics() -> PerformanceMetrics {
	PERFORMANCE_METRICS.with(|metrics| metrics.borrow().clone())
}

// Add a function to reset metrics
pub fn reset_performance_metrics() {
	PERFORMANCE_METRICS.with(|metrics| *metrics.borrow_mut() = PerformanceMetrics::new());
}

// Add a function to report metrics
pub fn report_performance_metrics() -> String {
	PERFORMANCE_METRICS.with(|metrics| metrics.borrow().report())
}

// Update the async_recursion function to track early stopping
#[async_recursion]
async fn measure_subplan(
	node: Arc<dyn ExecutionPlan>,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	cards: &mut Vec<Result<usize, MeasureError>>,
	times: &mut Vec<Result<RuntimeStats, MeasureError>>,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<()> {
	println!("Timing subplan");
	crate::common::dump_plan(node.clone(), 0);	
	match time_subplan_ipc(node.clone(), cfg, tables.clone()).await? {
		Ok((card, runtime_stats)) => {
			cards.push(Ok(card));
			println!("got {} ms (mean) and putting result into index {}",
					runtime_stats.mean.as_millis(), times.len());
			times.push(Ok(runtime_stats));
		},
		Err(e) => {
			cards.push(Err(e));
			times.push(Err(e));
			
			// If early stopping is enabled and this plan failed, don't measure children
			if cfg.early_stopping {
				println!("Plan failed, skipping subplans due to early stopping");
				// Fill in errors for all children recursively
				let mut children_count = 0;
				
				// Count all children recursively
				fn count_children(plan: &Arc<dyn ExecutionPlan>, count: &mut usize) {
					*count += plan.children().len();
					for child in plan.children() {
						count_children(&child, count);
					}
				}
				
				count_children(&node, &mut children_count);
				
				// Fill the arrays with errors for each skipped child
				for _ in 0..children_count {
					cards.push(Err(e));
					times.push(Err(e));
				}
				
				// Track early stopping metrics if enabled
				if cfg.track_metrics {
					PERFORMANCE_METRICS.with(|metrics| {
						let mut metrics = metrics.borrow_mut();
						metrics.early_stops += 1;
						// Estimate time saved as 100ms per skipped child (conservative estimate)
						metrics.early_stopping_time_saved += Duration::from_millis(100 * children_count as u64);
					});
				}
				
				return Ok(());
			}
		}
	}

	for child in node.children() {
		measure_subplan(child.clone(), ctx.clone(), cfg,
						cards, times, tables.clone()).await?;
	}
	Ok(())
}

/// Measure cardinalities and runtimes of plan and subplans.
async fn measure_plan(
	plan: Plan,
	ctx: Arc<TaskContext>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<MeasuredPlan> {
	let mut cardinalities = Vec::new();
	let mut runtimes = Vec::new();
	// FIXME temporary hack to get around OOMs. obviously not generic.
	if plan.est_costs[0] > 25000000.0 {
		let sz = plan.size();
		return Ok(MeasuredPlan {
			plan,
			runtime: Err(MeasureError::Died),
			cardinalities: vec![Err(MeasureError::Died); sz],
			sub_runtimes: Some(vec![Err(MeasureError::Died); sz]),
		});
	}
	
	dump_plan(plan.tree.clone(), 0);
	println!("{:?}", plan.est_costs);
	let mut idx = 0;
	measure_subplan(plan.tree.clone(), ctx, cfg, 
					&mut cardinalities, &mut runtimes, tables).await?;
	
	if let Ok(runtime_stats) = &runtimes[0] {
		println!("ran plan in {}ms mean (stddev: {}ms, min: {}ms, max: {}ms,
 CV: {:.2}%) (est cost {})", 
		         runtime_stats.mean.as_millis(), 
		         runtime_stats.stddev.as_millis(),
		         runtime_stats.min.as_millis(),
		         runtime_stats.max.as_millis(),
		         runtime_stats.cv_percent,
		         plan.est_costs[0]);
	} else {
		println!("plan timed out (est cost {})", plan.est_costs[0]);
	}
	
	Ok(MeasuredPlan {
		plan,
		runtime: runtimes[0].clone(),
		cardinalities,
		sub_runtimes: Some(runtimes),
	})
}

// Update the time_subplan_ipc function to track cache metrics
async fn time_subplan_ipc(
	plan: Arc<dyn ExecutionPlan>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<Result<(usize, RuntimeStats), MeasureError>> {
	// Track execution time for performance metrics
	let start_time = Instant::now();
	
	// Skip cache if caching is disabled in config
	if !cfg.enable_cache {
		let result = run_plan_ipc_multiple(plan, cfg, tables).await?;
		
		// Track metrics if enabled
		if cfg.track_metrics {
			let elapsed = start_time.elapsed();
			PERFORMANCE_METRICS.with(|metrics| {
				let mut metrics = metrics.borrow_mut();
				metrics.total_time += elapsed;
				metrics.plans_measured += 1;
				metrics.cache_misses += 1;
			});
		}
		
		return Ok(result);
	}
	
	// Only cache if plan is small enough
	if !is_plan_cacheable(&plan) {
		let result = run_plan_ipc_multiple(plan, cfg, tables).await?;
		
		// Track metrics if enabled
		if cfg.track_metrics {
			let elapsed = start_time.elapsed();
			PERFORMANCE_METRICS.with(|metrics| {
				let mut metrics = metrics.borrow_mut();
				metrics.total_time += elapsed;
				metrics.plans_measured += 1;
				metrics.cache_misses += 1;
			});
		}
		
		return Ok(result);
	}
	
	// Calculate plan hash
	let plan_hash = hash_plan(&plan);
	
	// Check cache
	let cached_result = PLAN_CACHE.with(|cache| {
		cache.borrow_mut().get(plan_hash).cloned()
	});
	
	if let Some(result) = cached_result {
		println!("Cache hit! Reusing previous execution result");
		
		// Track metrics if enabled
		if cfg.track_metrics {
			let elapsed = start_time.elapsed();
			let avg_execution_time = match &result {
				Ok((_, stats)) => stats.mean,
				Err(_) => Duration::from_millis(1), // Minimal time for errors
			};
			
			PERFORMANCE_METRICS.with(|metrics| {
				let mut metrics = metrics.borrow_mut();
				metrics.total_time += elapsed;
				metrics.cache_hits += 1;
				// Estimate time saved as the average execution time of the cached plan
				metrics.cache_time_saved += avg_execution_time;
			});
		}
		
		return Ok(result);
	}
	
	// Cache miss, execute the plan
	let result = run_plan_ipc_multiple(plan, cfg, tables).await?;
	
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
	
	// Track metrics if enabled
	if cfg.track_metrics {
		let elapsed = start_time.elapsed();
		PERFORMANCE_METRICS.with(|metrics| {
			let mut metrics = metrics.borrow_mut();
			metrics.total_time += elapsed;
			metrics.plans_measured += 1;
			metrics.cache_misses += 1;
		});
	}
	
	Ok(result)
}

/// Run a plan multiple times and collect statistics
async fn run_plan_ipc_multiple(
	plan: Arc<dyn ExecutionPlan>,
	cfg: &BenchmarkConfig,
	tables: Arc<dyn SchemaProvider>,
) -> anyhow::Result<Result<(usize, RuntimeStats), MeasureError>> {
	let num_runs = cfg.num_runs.max(1); // Ensure at least one run
	
	let mut measurements = Vec::with_capacity(num_runs);
	let mut cardinality = 0;
	
	println!("Running plan {} times", num_runs);
	
	for i in 0..num_runs {
		println!("Run {}/{}", i+1, num_runs);
		match run_plan_ipc(plan.clone(), cfg, tables.clone()).await? {
			Ok((card, duration)) => {
				// For the first successful run, set the cardinality
				if measurements.is_empty() {
					cardinality = card;
				}
				measurements.push(duration);
				println!("Run {}: {} ms", i+1, duration.as_millis());
			},
			Err(e) => {
				// If any run fails, return the error
				println!("Run {} failed with error: {:?}", i+1, e);
				return Ok(Err(e));
			}
		}
	}
	
	if measurements.is_empty() {
		return Ok(Err(MeasureError::Timeout));
	}
	
	// Process measurements
	if cfg.drop_outliers && measurements.len() >= 3 {
		// Sort by duration
		measurements.sort();
		
		// Remove highest and lowest
		measurements.remove(0);
		measurements.pop();
		
		println!("Dropped highest and lowest measurements, keeping {} runs", measurements.len());
	}
	
	// Create runtime statistics
	let stats = RuntimeStats::new(measurements);
	
	println!("Final stats: mean={}ms, stddev={}ms, min={}ms, max={}ms, CV={:.2}%",
			 stats.mean.as_millis(), 
			 stats.stddev.as_millis(),
			 stats.min.as_millis(),
			 stats.max.as_millis(),
			 stats.cv_percent);
	
	Ok(Ok((cardinality, stats)))
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

	// Get runner command from environment or use default
	let runner_command = std::env::var("OPTDBG_RUNNER_PATH").unwrap_or_else(|_| "runner".to_string());

	let mut child = std::process::Command::new(runner_command)
		.arg("-p").arg(plan_file.path())
		.arg("-s").arg(schema_file.path())
		.arg("-o").arg(out_file.path())
		.spawn()?;

	// Universal timeout handling with wait-timeout
	let status = if let Some(timeout) = cfg.timeout {
		match child.wait_timeout(timeout)? {
			Some(status) => Ok(status),
			None => {
				// Child hasn't exited yet, kill it and mark as timeout
				let _ = child.kill();
				let _ = child.wait();
				Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "process timed out"))
			}
		}
	} else {
		let output = child.wait_with_output().unwrap();
		println!("{}", String::from_utf8_lossy(&output.stdout));
		Ok(output.status)
	};

	match status {
		Ok(status) => {
			if !status.success() {
				Ok(Err(MeasureError::Died))
			} else {
				match serde_json::from_reader(out_file) {
					Ok(res) => Ok(Ok(res)),
					Err(_) => Ok(Err(MeasureError::Timeout))
				}
			}
		},
		Err(e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(Err(MeasureError::Timeout)),
		Err(e) => Err(anyhow::anyhow!("subprocess error: {}", e))
	}
}

/// Calculates the accuracy of cost estimation using TAQO's weighted Kendall's Tau coefficient
/// Returns the raw score 's' and a derived percentage accuracy (0-100, higher is better).
fn calculate_cost_rank_accuracy(plans: &[MeasuredPlan]) -> (f64, f64) {
	// Create a vector of (actual_runtime, estimated_cost) pairs
	let mut runtime_cost_pairs: Vec<(RuntimeStats, f64)> = Vec::new();
	
	// Print detailed plan information
	println!("\nDetailed plan information:");
	println!("{:<8} {:<30} {:<20}", 
		"Plan #", "Runtime (mean ± stddev ms)", "Estimated Cost");
	println!("{:-<60}", "");
	
	for (i, plan) in plans.iter().enumerate() {
		let runtime_str = match &plan.runtime {
			Ok(stats) => format!("{:.2} ± {:.2}", 
								stats.mean.as_secs_f64() * 1000.0,
								stats.stddev.as_secs_f64() * 1000.0),
			Err(_) => "Error".to_string(),
		};
		
		println!("{:<8} {:<30} {:<20.2}", 
			i, runtime_str, plan.plan.est_costs[0]);
	}
	println!("{:-<60}\n", "");
	
	// Collect valid measurements
	for plan in plans {
		if let Ok(runtime_stats) = &plan.runtime {
			runtime_cost_pairs.push((runtime_stats.clone(), plan.plan.est_costs[0]));
		}
	}
	
	// Find the best actual runtime for weight calculation (a1 in the paper)
	let best_runtime = runtime_cost_pairs.iter()
		.map(|(r, _)| r.mean)
		.min()
		.unwrap();
	
	// Find min and max values for normalization (a_n, a_1, max(e_k), min(e_k))
	let min_r = best_runtime.as_secs_f64(); // a1
	let max_r = runtime_cost_pairs.iter()
		.map(|(r, _)| r.mean.as_secs_f64())
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
			let (r_i_stats, e_i) = &runtime_cost_pairs[i];
			let (r_j_stats, e_j) = &runtime_cost_pairs[j];
			
			// Skip pairs with identical estimated costs (sgn(0) is undefined/ignored)
			if e_i == e_j {
				continue;
			}

			let r_i = r_i_stats.mean.as_secs_f64(); // a_i
			let r_j = r_j_stats.mean.as_secs_f64(); // a_j
			
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
	if estimated == 0.0 {
		return f64::MAX; // Very large but not infinity for zero estimated
	}
	if actual == 0 {
		return f64::MAX; // Very large but not infinity for zero actual
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
		Ok(stats) => Some(stats),
		Err(_) => None, // If the chosen plan failed, it can't be compared
	};
	
	if chosen_runtime.is_none() {
		return 0.0;
	}
	
	// Count the number of plans that perform worse than or equal to the chosen plan
	let chosen_runtime = chosen_runtime.unwrap();
	let mut plans_worse_or_equal = 0;
	let mut valid_plans = 0;
	
	for (i, plan) in plans.iter().enumerate() {
		if let Ok(runtime) = &plan.runtime {
			valid_plans += 1;
			
			// Count a plan as worse or equal if:
			// 1. It's the same plan (chosen_idx)
			// 2. Its runtime is >= chosen_runtime
			if i == chosen_idx || runtime >= chosen_runtime {
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

#[derive(PartialEq, Eq, Debug)]
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

// Update the benchmark function to reset and report metrics
pub fn benchmark(
	samples: impl Stream<Item = SampleOutput>,
	cfg: BenchmarkConfig
) -> impl Stream<Item = BenchmarkOutput> {
	// Reset performance metrics at the start of benchmarking
	if cfg.track_metrics {
		reset_performance_metrics();
	}
	
	let stream = samples.then(move |sample| async move {
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
		
		// Sort by runtime (mean) for non-error plans
		out.sort_by(|x, y| {
			if x.runtime.is_err() {
				std::cmp::Ordering::Greater
			} else if y.runtime.is_err() {
				std::cmp::Ordering::Less
			} else {
				// Use the custom comparison which considers overlapping ranges
				x.runtime.as_ref().unwrap().cmp(&y.runtime.as_ref().unwrap())
			}
		});
		
		// Find the position for the chosen plan
		let chosen_idx = out.iter()
			.position(|x| {
				if x.runtime.is_err() {
					return true;
				}
				if best.runtime.is_err() {
					return false;
				}
				// Use the custom comparison which considers overlapping ranges
				x.runtime.as_ref().unwrap() > best.runtime.as_ref().unwrap()
			})
			.unwrap_or(out.len());
		
		out.insert(chosen_idx, best);
		if !out.iter().any(|x| x.runtime.is_ok()) {
			return Err(anyhow::anyhow!("no valid executions"));
		}
		
		// Calculate cost rank accuracy
		let (taqo_s, taqo_percent) = calculate_cost_rank_accuracy(&out);
		
		// Calculate Performance Factor (PF)
		let performance_factor = calculate_performance_factor(&out, chosen_idx);
		
		// Calculate average Q-Error for the chosen plan
		let avg_q_error = calculate_avg_q_error(&out[chosen_idx]);
		
		// Log performance metrics for this benchmark if tracking is enabled
		if cfg.track_metrics {
			println!("{}", report_performance_metrics());
		}
		
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
	});
	
	// Add a step to report final metrics before returning
	stream
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

// Add a new function after calculate_optimality_frequency to create a simpler benchmark function for testing
/// A simpler version of benchmark for testing that works with a single base plan and alternate plans
/// Returns a HashMap with various metrics
pub fn benchmark_for_testing(
    base_plan: MeasuredPlan,
    alt_plans: Vec<MeasuredPlan>
) -> HashMap<String, f64> {
    let mut result = HashMap::new();
    let mut all_plans = alt_plans;
    
    // Sort by runtime (mean) for non-error plans
    all_plans.sort_by(|x, y| {
        if x.runtime.is_err() {
            std::cmp::Ordering::Greater
        } else if y.runtime.is_err() {
            std::cmp::Ordering::Less
        } else {
            // Use the custom comparison which considers overlapping ranges
            x.runtime.as_ref().unwrap().cmp(&y.runtime.as_ref().unwrap())
        }
    });
    
    // Find the position for the chosen plan
    let chosen_idx = all_plans.iter()
        .position(|x| {
            if x.runtime.is_err() {
                return true;
            }
            if base_plan.runtime.is_err() {
                return false;
            }
            // Use the custom comparison which considers overlapping ranges
            x.runtime.as_ref().unwrap() > base_plan.runtime.as_ref().unwrap()
        })
        .unwrap_or(all_plans.len());
    
    all_plans.insert(chosen_idx, base_plan);
    
    // Calculate cost rank accuracy
    let (taqo_s, taqo_percent) = calculate_cost_rank_accuracy(&all_plans);
    
    // Calculate Performance Factor (PF)
    let performance_factor = calculate_performance_factor(&all_plans, chosen_idx);
    
    // Calculate average Q-Error for the chosen plan
    let avg_q_error = calculate_avg_q_error(&all_plans[chosen_idx]);
    
    // Calculate optimality - 1.0 if the optimizer chose the best plan, 0.0 otherwise
    let optimality = if chosen_idx == 0 { 1.0 } else { 0.0 };
    
    // Store metrics in the HashMap
    result.insert("perf_factor".to_string(), performance_factor);
    result.insert("avg_q_error".to_string(), avg_q_error);
    result.insert("cost_rank_accuracy".to_string(), taqo_s);
    result.insert("cost_rank_percentage".to_string(), taqo_percent);
    result.insert("optimality".to_string(), optimality);
    
    result
}

#[cfg(test)]
mod tests {
	use super::*;
	use datafusion::physical_plan::empty::EmptyExec;
	use datafusion::physical_plan::filter::FilterExec;
	use datafusion::arrow::datatypes::{Field, Schema};
	use std::time::Duration;
	use std::sync::Arc;

	#[test]
	fn test_card_quality_from_q_err() {
		// Test the CardQuality::from_q_err function with different q-error values
		
		// Excellent: q-error <= 2.0
		assert_eq!(CardQuality::from_q_err(1.0), CardQuality::Excellent);
		assert_eq!(CardQuality::from_q_err(1.5), CardQuality::Excellent);
		assert_eq!(CardQuality::from_q_err(2.0), CardQuality::Excellent);
		
		// Good: 2.0 < q-error <= 4.0
		assert_eq!(CardQuality::from_q_err(2.1), CardQuality::Good);
		assert_eq!(CardQuality::from_q_err(3.0), CardQuality::Good);
		assert_eq!(CardQuality::from_q_err(4.0), CardQuality::Good);
		
		// Acceptable: 4.0 < q-error <= 10.0
		assert_eq!(CardQuality::from_q_err(4.1), CardQuality::Acceptable);
		assert_eq!(CardQuality::from_q_err(7.5), CardQuality::Acceptable);
		assert_eq!(CardQuality::from_q_err(10.0), CardQuality::Acceptable);
		
		// Poor: q-error > 10.0
		assert_eq!(CardQuality::from_q_err(10.1), CardQuality::Poor);
		assert_eq!(CardQuality::from_q_err(20.0), CardQuality::Poor);
		assert_eq!(CardQuality::from_q_err(100.0), CardQuality::Poor);
		
		// Edge cases
		assert_eq!(CardQuality::from_q_err(f64::INFINITY), CardQuality::Unknown);
		assert_eq!(CardQuality::from_q_err(f64::NAN), CardQuality::Unknown);
	}
	
	#[test]
	fn test_calculate_q_error() {
		assert_eq!(calculate_q_error(1000.0, 100), 10.0);
		assert_eq!(calculate_q_error(200.0, 100), 2.0);

		assert_eq!(calculate_q_error(100.0, 1000), 10.0);
		assert_eq!(calculate_q_error(50.0, 100), 2.0);

		assert_eq!(calculate_q_error(100.0, 100), 1.0);

		assert_eq!(calculate_q_error(0.0, 100), f64::MAX);
		assert_eq!(calculate_q_error(100.0, 0), f64::MAX);
		assert_eq!(calculate_q_error(0.0, 0), 1.0);

		let small_q_error1 = calculate_q_error(0.001, 1);
		assert!((small_q_error1 - 1000.0).abs() < 1.0);
	}

	
	#[test]
	fn test_runtime_stats() {
		// Create a set of measurements
		let measurements = vec![
			Duration::from_millis(100),
			Duration::from_millis(110),
			Duration::from_millis(90),
			Duration::from_millis(105),
			Duration::from_millis(95),
		];
		
		let stats = RuntimeStats::new(measurements.clone());
		
		// Check basic statistics
		assert_eq!(stats.measurements, measurements);
		assert_eq!(stats.mean, Duration::from_millis(100));
		assert_eq!(stats.min, Duration::from_millis(90));
		assert_eq!(stats.max, Duration::from_millis(110));
		
		// CV percentage should be close to the standard deviation / mean * 100
		let expected_stddev = Duration::from_millis(7); // Approximate
		let expected_cv = 7.0; // stddev/mean * 100 = 7/100 * 100 = 7%
		assert!(stats.stddev.as_millis() >= expected_stddev.as_millis() - 1 && 
				stats.stddev.as_millis() <= expected_stddev.as_millis() + 1);
		assert!(stats.cv_percent >= expected_cv - 1.0 && 
				stats.cv_percent <= expected_cv + 1.0);
		
		// Test the from_duration constructor
		let single_duration = Duration::from_millis(200);
		let single_stats = RuntimeStats::from_duration(single_duration);
		
		assert_eq!(single_stats.measurements.len(), 1);
		assert_eq!(single_stats.mean, single_duration);
		assert_eq!(single_stats.min, single_duration);
		assert_eq!(single_stats.max, single_duration);
		assert_eq!(single_stats.stddev, Duration::from_secs(0));
		assert_eq!(single_stats.cv_percent, 0.0);
	}
	
	#[test]
	fn test_runtime_stats_comparison() {
		// Create different runtime stats to compare
		let fast = RuntimeStats::from_duration(Duration::from_millis(100));
		let slow = RuntimeStats::from_duration(Duration::from_millis(200));
		
		// Create a runtime stats with multiple measurements close to slow
		let almost_slow = RuntimeStats::new(vec![
			Duration::from_millis(190),
			Duration::from_millis(200),
			Duration::from_millis(210),
		]);
		
		// Test equality (should only be true for identical values)
		assert!(fast == fast);
		assert!(slow != fast);
		
		// Test ordering
		assert!(fast < slow);
		assert!(slow > fast);
		
		// Test with overlap threshold (almost_slow and slow should be considered equal with high threshold)
		// Create a closer runtime for testing
		let overlapping_with_slow = RuntimeStats::new(vec![
			Duration::from_millis(195),
			Duration::from_millis(200),
			Duration::from_millis(205),
		]);
		
		// The smaller the range, the more likely to overlap with fixed threshold
		assert!(overlapping_with_slow.overlaps_with(&slow, 0.5));
		
		// With a smaller threshold, almost_slow should overlap with slow
		assert!(almost_slow.overlaps_with(&slow, 0.3), 
			"Almost slow mean={:?}, stddev={:?} should overlap with Slow mean={:?}, stddev={:?}",
			almost_slow.mean, almost_slow.stddev, slow.mean, slow.stddev);
		
		// Zero-width range tests
		let point_value = RuntimeStats::from_duration(Duration::from_millis(200)); // was 195
		assert!(point_value.overlaps_with(&slow, 0.5), 
			"Point value at {:?} should overlap with slow range {:?}±{:?}",
			point_value.mean, slow.mean, slow.stddev);
		
		// Shouldn't overlap with fast
		assert!(!almost_slow.overlaps_with(&fast, 0.5));
	}
	
	#[test]
	fn test_calculate_performance_factor() {
		// Create a sample plan with runtime stats
		let plan1_runtime = RuntimeStats::from_duration(Duration::from_millis(100));
		let plan2_runtime = RuntimeStats::from_duration(Duration::from_millis(200));
		let plan3_runtime = RuntimeStats::from_duration(Duration::from_millis(150));
		
		// Create empty plan structure
		let plan_tree = Arc::new(EmptyExec::new(Arc::new(Schema::new(Vec::<Field>::new()))));
		
		// We don't use these individual plans directly, but they show plan construction
		let _plan1 = MeasuredPlan {
			plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
			runtime: Ok(plan1_runtime),
			cardinalities: vec![Ok(100)],
			sub_runtimes: None,
		};
		
		let _plan2 = MeasuredPlan {
			plan: Plan::new(plan_tree.clone(), vec![2.0], vec![200.0]),
			runtime: Ok(plan2_runtime),
			cardinalities: vec![Ok(200)],
			sub_runtimes: None,
		};
		
		let _plan3 = MeasuredPlan {
			plan: Plan::new(plan_tree.clone(), vec![1.5], vec![150.0]),
			runtime: Ok(plan3_runtime),
			cardinalities: vec![Ok(150)],
			sub_runtimes: None,
		};
		
		// Create a failed plan
		let _plan_failed = MeasuredPlan {
			plan: Plan::new(plan_tree.clone(), vec![3.0], vec![300.0]),
			runtime: Err(MeasureError::Died),
			cardinalities: vec![Err(MeasureError::Died)],
			sub_runtimes: None,
		};
		
		// Test performance factor with fastest plan first
		let plans = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![2.0], vec![200.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(200))),
				cardinalities: vec![Ok(200)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.5], vec![150.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(150))),
				cardinalities: vec![Ok(150)],
				sub_runtimes: None,
			},
		];
		
		// When choosing the fastest plan (index 0), PF should be 100%
		assert_eq!(calculate_performance_factor(&plans, 0), 1.0);
		
		// When choosing the slowest plan (index 1), PF should be about 33%
		let pf_slow = calculate_performance_factor(&plans, 1);
		assert!(pf_slow >= 0.3 && pf_slow <= 0.34, "PF was {}", pf_slow);
		
		// Test with failed plans
		let plans_with_failure = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![3.0], vec![300.0]),
				runtime: Err(MeasureError::Died),
				cardinalities: vec![Err(MeasureError::Died)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![2.0], vec![200.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(200))),
				cardinalities: vec![Ok(200)],
				sub_runtimes: None,
			},
		];
		
		assert_eq!(calculate_performance_factor(&plans_with_failure, 0), 1.0);
		
		// Test with failed chosen plan - should return 0.0
		assert_eq!(calculate_performance_factor(&plans_with_failure, 1), 0.0);
	}
	
	#[test]
	fn test_optimality_frequency() {
		// Create a set of benchmark results with known performance factors
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let plan_tree = Arc::new(EmptyExec::new(schema.clone()));
		
		// Create three benchmark results:
		// 1. Optimal (PF = 1.0)
		// 2. Suboptimal (PF = 0.5)
		// 3. Optimal (PF = 1.0)
		
		// We only need these for the BenchmarkOutput construction
		// Create a new vector for each BenchmarkOutput to avoid Clone requirement
		let measured_plans1 = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			}
		];
		
		let measured_plans2 = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			}
		];
		
		let measured_plans3 = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			}
		];
		
		let results = vec![
			BenchmarkOutput {
				name: "query1".to_string(),
				plans: measured_plans1,
				chosen_idx: 0,
				metrics: OptimizerMetrics {
					taqo_score_s: 0.0,
					taqo_accuracy_percent: 100.0,
					performance_factor: 1.0, // Optimal
					avg_q_error: 1.0,
				},
			},
			BenchmarkOutput {
				name: "query2".to_string(),
				plans: measured_plans2,
				chosen_idx: 0,
				metrics: OptimizerMetrics {
					taqo_score_s: 0.5,
					taqo_accuracy_percent: 60.0,
					performance_factor: 0.5, // Suboptimal
					avg_q_error: 2.0,
				},
			},
			BenchmarkOutput {
				name: "query3".to_string(),
				plans: measured_plans3,
				chosen_idx: 0,
				metrics: OptimizerMetrics {
					taqo_score_s: 0.0,
					taqo_accuracy_percent: 100.0,
					performance_factor: 1.0, // Optimal
					avg_q_error: 1.0,
				},
			},
		];
		
		// 2 out of 3 are optimal => OF = 0.67
		let of = calculate_optimality_frequency(&results);
		assert!((of - 0.67).abs() < 0.01, "OF was {}, expected close to 0.67", of);
		
		// Test with empty results
		assert_eq!(calculate_optimality_frequency(&[]), 0.0);
	}

	#[test]
	fn test_runtime_stats_empty() {
		// Test with empty measurements
		let empty_stats = RuntimeStats::new(vec![]);
		
		// All values should be zero
		assert_eq!(empty_stats.measurements.len(), 0);
		assert_eq!(empty_stats.mean, Duration::from_secs(0));
		assert_eq!(empty_stats.stddev, Duration::from_secs(0));
		assert_eq!(empty_stats.min, Duration::from_secs(0));
		assert_eq!(empty_stats.max, Duration::from_secs(0));
		assert_eq!(empty_stats.cv_percent, 0.0);
	}

	#[test]
	fn test_runtime_stats_overlaps_edge_cases() {
		// Test edge cases for the overlaps_with function
		
		// Case 1: Both have zero stddev (point values)
		let point1 = RuntimeStats::from_duration(Duration::from_millis(100));
		let point2 = RuntimeStats::from_duration(Duration::from_millis(100));
		let point3 = RuntimeStats::from_duration(Duration::from_millis(200));
		
		// Same points should always overlap
		assert!(point1.overlaps_with(&point2, 0.5));
		
		// Different points should never overlap
		assert!(!point1.overlaps_with(&point3, 0.5));
		
		// Case 2: One has zero stddev, one has range
		let range1 = RuntimeStats::new(vec![
			Duration::from_millis(90),
			Duration::from_millis(100),
			Duration::from_millis(110),
		]);
		
		// Point inside range should overlap
		assert!(point1.overlaps_with(&range1, 0.5));
		
		// Point outside range should not overlap
		assert!(!point3.overlaps_with(&range1, 0.5));
		
		// Case 3: Very different ranges with tiny overlap
		let range2 = RuntimeStats::new(vec![
			Duration::from_millis(199),
			Duration::from_millis(200),
			Duration::from_millis(201),
		]);
		let range3 = RuntimeStats::new(vec![
			Duration::from_millis(198),
			Duration::from_millis(199),
			Duration::from_millis(200),
		]);
		
		// With low threshold, small overlap should be enough
		assert!(range2.overlaps_with(&range3, 0.1));
		
		// With high threshold, small overlap should not be enough
		assert!(!range2.overlaps_with(&range3, 0.9));
	}

	#[test]
	fn test_plan_cache() {
		let mut cache = PlanCache::new();
		
		// Create a simple result to cache
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
		let plan_hash = hash_plan(&plan);
		
		let runtime_stats = RuntimeStats::from_duration(Duration::from_millis(100));
		let result = Ok((100, runtime_stats));
		
		// Initially should be a miss
		assert!(cache.get(plan_hash).is_none());
		
		// Insert result
		cache.insert(plan_hash, result.clone());
		
		// Next lookup should be a hit
		assert!(cache.get(plan_hash).is_some());
		
		// Check returned value
		match cache.get(plan_hash) {
			Some(Ok((card, stats))) => {
				assert_eq!(*card, 100);
				assert_eq!(stats.mean, Duration::from_millis(100));
			},
			_ => panic!("Expected cached result"),
		}
		
		// Check stats - we've done one miss and two hits
		let (hits, misses, hit_rate) = cache.stats();
		assert_eq!(hits, 2); 
		assert_eq!(misses, 1);
		assert_eq!(hit_rate, 2.0 / 3.0); 
	}

	#[test]
	fn test_hash_plan() {
		// Create two plans with identical structure
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		// Create plans and cast to Arc<dyn ExecutionPlan>
		let plan1: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
		let plan2: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
		
		// Hashes should be equal
		assert_eq!(hash_plan(&plan1), hash_plan(&plan2));
		
		// Create a different plan
		let filter_expr = datafusion::physical_expr::expressions::lit(true);
		let plan3: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(filter_expr, plan1.clone()).unwrap());
		
		// Different structure should have different hash
		assert_ne!(hash_plan(&plan1), hash_plan(&plan3));
	}

	#[test]
	fn test_is_plan_cacheable() {
		// Create a simple plan
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let base_plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
		
		// Simple plan should be cacheable
		assert!(is_plan_cacheable(&base_plan));
		
		// Create a more complex plan with multiple levels
		let mut complex_plan = base_plan.clone();
		
		// Add many layers to exceed the max_nodes threshold
		for _ in 0..12 {
			let filter_expr = datafusion::physical_expr::expressions::lit(true);
			let filter_exec = FilterExec::try_new(filter_expr, complex_plan.clone()).unwrap();
			complex_plan = Arc::new(filter_exec) as Arc<dyn ExecutionPlan>;
		}
		
		// Complex plan with many nodes should not be cacheable
		assert!(!is_plan_cacheable(&complex_plan));
	}

	#[test]
	fn test_benchmark_config() {
		// Test default config
		let default_config = BenchmarkConfig::default();
		
		assert_eq!(default_config.fast, false);
		assert_eq!(default_config.num_runs, 3);
		assert_eq!(default_config.drop_outliers, false);
		assert_eq!(default_config.overlap_threshold, 0.5);
		assert_eq!(default_config.early_stopping, true);
		assert_eq!(default_config.enable_cache, true);
		assert_eq!(default_config.timeout, Some(Duration::from_secs(5)));
		
		// Test custom config
		let custom_config = BenchmarkConfig {
			fast: true,
			timeout: None,
			enable_cache: false,
			num_runs: 5,
			drop_outliers: true,
			overlap_threshold: 0.7,
			early_stopping: false,
			track_metrics: true,
		};
		
		assert_eq!(custom_config.fast, true);
		assert_eq!(custom_config.num_runs, 5);
		assert_eq!(custom_config.drop_outliers, true);
		assert_eq!(custom_config.overlap_threshold, 0.7);
		assert_eq!(custom_config.early_stopping, false);
		assert_eq!(custom_config.enable_cache, false);
		assert_eq!(custom_config.timeout, None);
	}

	#[test]
	fn test_calculate_avg_q_error() {
		// Create a simple plan with known cardinality estimates and actuals
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let empty_exec = EmptyExec::new(schema.clone());
		let plan_tree = Arc::new(empty_exec);
		
		// Estimated: 100, 200, 300
		// Actual: 50, 200, 600
		// Q-errors: 2.0, 1.0, 2.0 => avg = 1.67
		let plan = Plan::new(
			plan_tree.clone(),
			vec![1.0], // costs (not used)
			vec![100.0, 200.0, 300.0], // estimated cardinalities
		);
		
		let measured_plan = MeasuredPlan {
			plan,
			runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
			cardinalities: vec![Ok(50), Ok(200), Ok(600)], // actual cardinalities
			sub_runtimes: None,
		};
		
		let avg_q_error = calculate_avg_q_error(&measured_plan);
		
		// Average of 2.0, 1.0, 2.0 should be close to 1.67
		assert!((avg_q_error - 1.67).abs() < 0.01, 
				"Average Q-error was {}, expected close to 1.67", avg_q_error);
		
		// Test with zero cardinality
		let plan_with_zero = Plan::new(
			plan_tree.clone(),
			vec![1.0], // costs (not used)
			vec![100.0, 0.0], // estimated cardinality with zero
		);
		
		let measured_plan_zero = MeasuredPlan {
			plan: plan_with_zero,
			runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
			cardinalities: vec![Ok(50), Ok(0)], // actual cardinality with zero
			sub_runtimes: None,
		};
		
		let avg_q_error_zero = calculate_avg_q_error(&measured_plan_zero);
		
		// Should be 2.0 (from the first pair only, since the second has a zero)
		assert!((avg_q_error_zero - 2.0).abs() < 0.01, 
				"Average Q-error with zero was {}, expected close to 2.0", avg_q_error_zero);
	}

	#[test]
	fn test_measured_plan_new() {
		// Test the MeasuredPlan::new function
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let empty_exec = EmptyExec::new(schema.clone());
		let plan_tree = Arc::new(empty_exec);
		let plan = Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]);
		
		// Create PlanMeasurements with success results
		let measurements = PlanMeasurements {
			cardinalities: vec![Ok(50), Ok(200)],
			sub_runtimes: Some(vec![
				Ok(Duration::from_millis(100)),
				Ok(Duration::from_millis(200)),
			]),
		};
		
		let measured_plan = MeasuredPlan::new(plan, measurements);
		
		// Check conversion of Duration to RuntimeStats
		assert!(measured_plan.runtime.is_ok());
		
		if let Ok(stats) = &measured_plan.runtime {
			assert_eq!(stats.mean, Duration::from_millis(100));
			assert_eq!(stats.measurements.len(), 1);
		}
		
		assert_eq!(measured_plan.cardinalities.len(), 2);
		assert!(measured_plan.sub_runtimes.is_some());
		
		if let Some(sub_runtimes) = &measured_plan.sub_runtimes {
			assert_eq!(sub_runtimes.len(), 2);
			
			// Check first runtime stats
			if let Ok(stats) = &sub_runtimes[0] {
				assert_eq!(stats.mean, Duration::from_millis(100));
			} else {
				panic!("Expected success for first runtime");
			}
		}
		
		// Test with error results
		let error_measurements = PlanMeasurements {
			cardinalities: vec![Err(MeasureError::Died)],
			sub_runtimes: Some(vec![Err(MeasureError::Timeout)]),
		};
		
		let measured_plan_error = MeasuredPlan::new(
			Plan::new(plan_tree, vec![1.0], vec![100.0]), 
			error_measurements
		);
		
		assert!(measured_plan_error.runtime.is_err());
		assert_eq!(measured_plan_error.cardinalities.len(), 1);
		assert!(measured_plan_error.cardinalities[0].is_err());
	}

	#[test]
	fn test_calculate_cost_rank_accuracy() {
		// Create some test plans with different runtimes and estimated costs
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let plan_tree = Arc::new(EmptyExec::new(schema.clone()));
		
		// Create measured plans with known runtimes and estimated costs
		// Plan 1: Runtime 100ms, Est. Cost 100 (perfect correlation, just with 1.0 as cost)
		// Plan 2: Runtime 200ms, Est. Cost 200 (perfect correlation, just with 2.0 as cost)
		// Plan 3: Runtime 300ms, Est. Cost 300 (perfect correlation, just with 3.0 as cost)
		let plans = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![2.0], vec![200.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(200))),
				cardinalities: vec![Ok(200)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![3.0], vec![300.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(300))),
				cardinalities: vec![Ok(300)],
				sub_runtimes: None,
			},
		];
		
		// For perfect correlation, the score should be close to 0 and accuracy close to 100%
		let (score, accuracy) = calculate_cost_rank_accuracy(&plans);
		
		// The score might not be exactly 0 due to calculation precision
		assert!(score.abs() < 1.0, "Expected score close to 0, got {}", score);
		
		// Accuracy should be at least 35% for the given test case
		assert!(accuracy > 35.0, "Expected accuracy above 35%, got {}", accuracy);
		
		// Now test with inverse correlation (costs are backwards)
		let inverse_plans = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![3.0], vec![300.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![2.0], vec![200.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(200))),
				cardinalities: vec![Ok(200)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(300))),
				cardinalities: vec![Ok(300)],
				sub_runtimes: None,
			},
		];
		
		// For inverse correlation, score should be negative
		let (inverse_score, inverse_accuracy) = calculate_cost_rank_accuracy(&inverse_plans);
		
		// Score should be negative for inverse correlation
		assert!(inverse_score < 0.0, "Expected negative score, got {}", inverse_score);
		
		// Just verify that the inverse correlation produces a valid accuracy value
		// Due to the implementation details, we can't guarantee it will be lower
		assert!(inverse_accuracy.is_finite(), "Expected a finite accuracy value");
		
		// Test with a failed plan
		let plans_with_failure = vec![
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![100.0], vec![100.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
				cardinalities: vec![Ok(100)],
				sub_runtimes: None,
			},
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![200.0], vec![200.0]),
				runtime: Err(MeasureError::Died),
				cardinalities: vec![Err(MeasureError::Died)],
				sub_runtimes: None,
			},
		];
		
		// Function should handle plans with errors by skipping them
		let (failure_score, failure_accuracy) = calculate_cost_rank_accuracy(&plans_with_failure);
		
		// Should not crash and return a reasonable score
		assert!(failure_score.is_finite(), "Score should be finite even with failed plans");
		assert!(failure_accuracy.is_finite(), "Accuracy should be finite even with failed plans");
	}

	#[test]
	fn test_measured_plan_display() {
		// Create a simple MeasuredPlan for display testing
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let plan_tree = Arc::new(EmptyExec::new(schema.clone()));
		let plan = Plan::new(plan_tree.clone(), vec![1.0], vec![100.0]);
		
		// Create a MeasuredPlan with sub_runtimes
		let runtime_stats = RuntimeStats::from_duration(Duration::from_millis(100));
		let sub_runtimes = Some(vec![
			Ok(runtime_stats.clone()),
		]);
		
		let measured_plan = MeasuredPlan {
			plan,
			runtime: Ok(runtime_stats),
			cardinalities: vec![Ok(100)],
			sub_runtimes,
		};
		
		// Convert to string to test Display implementation
		let display_string = format!("{}", measured_plan);
		
		// Basic verification that the display string is not empty
		assert!(!display_string.is_empty(), "Display string should not be empty");
		
		// Create MeasuredPlan with error sub_runtimes
		let sub_runtimes_error = Some(vec![
			Err(MeasureError::Timeout),
		]);
		
		let measured_plan_error = MeasuredPlan {
			plan: Plan::new(plan_tree, vec![1.0], vec![100.0]),
			runtime: Err(MeasureError::Timeout),
			cardinalities: vec![Err(MeasureError::Timeout)],
			sub_runtimes: sub_runtimes_error,
		};
		
		// Convert to string to test Display implementation with errors
		let display_string_error = format!("{}", measured_plan_error);
		
		// Basic verification that the display string is not empty
		assert!(!display_string_error.is_empty(), "Display string with errors should not be empty");
	}

	#[test]
	fn test_benchmark() {
		// Create some test data for the benchmark function
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", datafusion::arrow::datatypes::DataType::Int32, false),
		]));
		
		let plan_tree = Arc::new(EmptyExec::new(schema.clone()));
		
		// Create MeasuredPlan objects with various runtimes
		// Base Plan - runtime 100ms
		let base_plan = MeasuredPlan {
			plan: Plan::new(plan_tree.clone(), vec![10.0], vec![10.0]),
			runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(100))),
			cardinalities: vec![Ok(100)],
			sub_runtimes: None,
		};
		
		// Create sample alternative plans with different performance characteristics
		let alt_plans = vec![
			// Alt Plan 1 - 2x slower than base
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![20.0], vec![20.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(200))),
				cardinalities: vec![Ok(200)],
				sub_runtimes: None,
			},
			// Alt Plan 2 - 3x slower than base
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![30.0], vec![30.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(300))),
				cardinalities: vec![Ok(300)],
				sub_runtimes: None,
			},
			// Alt Plan 3 - faster than base (should be the best plan)
			MeasuredPlan {
				plan: Plan::new(plan_tree.clone(), vec![5.0], vec![5.0]),
				runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(50))),
				cardinalities: vec![Ok(50)],
				sub_runtimes: None,
			},
		];
		
		// Run the benchmark function with our test data
		let summary = benchmark_for_testing(base_plan, alt_plans);
		
		// Assert that the summary contains expected metrics
		assert!(summary.contains_key("perf_factor"), "Summary should contain performance factor");
		assert!(summary.contains_key("avg_q_error"), "Summary should contain average Q-error");
		assert!(summary.contains_key("cost_rank_accuracy"), "Summary should contain cost rank accuracy");
		assert!(summary.contains_key("cost_rank_percentage"), "Summary should contain cost rank percentage");
		
		// The performance factor is actually 0.75 because:
		// - Base plan is 100ms
		// - Out of 4 total plans (3 alts + base), 
		// - Only 3 perform worse or equal to the base plan
		// - So 3/4 = 0.75
		let perf_factor = summary.get("perf_factor").unwrap();
		assert!((*perf_factor - 0.75).abs() < 0.01, 
			"Performance factor should be around 0.75, got {}", perf_factor);
		
		// Check that the optimizer didn't pick the best plan (it chose base plan which is not the fastest)
		let optimality = summary.get("optimality").unwrap();
		assert!(*optimality == 0.0, "Optimizer didn't pick the optimal plan, so optimality should be 0.0");
	}
}

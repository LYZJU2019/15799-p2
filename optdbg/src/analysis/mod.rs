use std::{collections::HashSet, sync::Arc};

use datafusion::{common::HashMap, physical_expr::expressions::BinaryExpr, physical_plan::{filter::FilterExec, joins::{HashJoinExec, NestedLoopJoinExec}, ExecutionPlan, PhysicalExpr}};
use futures::{Stream, StreamExt};
use itertools::Itertools;

use crate::{
	benchmark::{calculate_q_error, BenchmarkOutput, CardQuality, MeasuredPlan, OptimizerMetrics},
	common::{partial_eq_plans, MeasureError}
};

pub struct AnalysisConfig {
	/// Whether to only consider "root" problems (those with good inputs) for bug detection.
	pub root_problems_only: bool,
}	

#[derive(Debug)]
pub enum NodeProblem {
	Crash,
	CardinalityMisestimation(usize, usize, f64),
	CostMisestimation(u128, f64, usize, usize),
}

pub struct Report {
	/// Optimality Frequency (OF) - fraction of queries for which the optimizer chooses the relative optimal plan
	pub opt_freq: f64,
	pub avg_taqo_score: f64,
	pub avg_taqo_acc: f64,
	pub avg_perf_factor: f64,
	pub queries: Vec<QueryReport>,
}

pub struct QueryReport {
	node_problem_frequency: HashMap<String, (usize, usize)>,
	pred_problem_frequency: HashMap<String, (usize, usize)>,
	pub optimal_chosen: bool,
	pub name: String,
	pub metrics: OptimizerMetrics,
	pub samples: Vec<MeasuredPlan>,
	/// Index of chosen plan in `samples` field.
	pub chosen: usize,
	pub node_problems: HashMap<usize, HashMap<usize, Vec<NodeProblem>>>
}

impl QueryReport {
	fn dump_plan_help(
		&self,
		f: &mut std::fmt::Formatter<'_>,
		node: Arc<dyn ExecutionPlan>,
		depth: usize,
		plan_idx: usize,
		idx: &mut usize,
	) -> std::fmt::Result {
		for _ in 0..depth {
			write!(f, "  ")?;
		}
		writeln!( 
			f,
			"{} | {}",
			node.name(),
			if let Some(x) = self.node_problems.get(&plan_idx).and_then(|x| x.get(idx)) {
				format!("{x:?}")
			} else { "".to_string() }
		)?;
		*idx += 1;
		for child in node.children() {
			self.dump_plan_help(
				f,
				child.clone(),
				depth + 1,
				plan_idx,
				idx,
			)?;
		}
		Ok(())
	}
	
	fn dump_plan(&self, f: &mut std::fmt::Formatter<'_>, idx: usize) -> std::fmt::Result {
		let mut node_idx = 0;
		self.dump_plan_help(
			f,
			self.samples[idx].plan.tree.clone(),
			0,
			idx,
			&mut node_idx
		)
	}
}

impl std::fmt::Display for Report {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		writeln!(f, "=================== GLOBAL STATS ===================\n")?;
		writeln!(f, "Evaluated {} queries.", self.queries.len())?;
		writeln!(f, "Optimality Frequency (OF): {:.2} (higher is better)", self.opt_freq * 100.0)?;
		writeln!(f, "Average TAQO Score (s): {:.4} (lower is better)", self.avg_taqo_score)?;
		writeln!(f, "Average TAQO Accuracy: {:.2}% (higher is better)", self.avg_taqo_acc)?;
		writeln!(f, "Average Performance Factor (PF): {:.2}%", self.avg_perf_factor * 100.0)?;
		writeln!(f, "\n====================================================\n\n")?;
		for q in &self.queries {
			writeln!(f, "{q}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for QueryReport {	
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		use asciigraph::*;
		writeln!(f, "------------------- {} -------------------\n", self.name)?;
		writeln!(f, "Sampled {} plans.", self.samples.len())?;
		writeln!(f, "TAQO Score (s): {:.4} (lower is better)", self.metrics.taqo_score_s)?;
		writeln!(f, "TAQO Accuracy: {:.2}% (higher is better)", self.metrics.taqo_accuracy_percent)?;
		writeln!(f, "Performance Factor (PF): {:.2}%", self.metrics.performance_factor * 100.0)?;
		writeln!(f, "Average Q-Error: {:.2} (closer to 1.0 is better)", self.metrics.avg_q_error)?;

		let mut perfgraph = Graph::default();
		let data: Vec<_> = self.samples
			.iter()
			.filter_map(|x| x.runtime.as_ref().ok().map(|y| y.mean.as_millis() as usize))
			.collect();
		perfgraph.set_1d_data(&data);
		writeln!(f, "{perfgraph}")?;
		
		writeln!(f, "")?;
		match CardQuality::from_q_err(self.metrics.avg_q_error) {
			CardQuality::Excellent =>
				writeln!(f, "Cardinality estimation is excellent (avg Q-Error ≤ 2.0)"),
			CardQuality::Good =>
				writeln!(f, "Cardinality estimation is good (avg Q-Error ≤ 4.0)"),
			CardQuality::Acceptable =>
				writeln!(f, "Cardinality estimation is acceptable (avg Q-Error ≤ 10.0)"),
			CardQuality::Poor => 
				writeln!(f, "Cardinality estimation needs improvement (avg Q-Error > 10.0)"),
			CardQuality::Unknown => 
				writeln!(f, "Cardinality estimation cannot be evaluated"),
		}?;
		writeln!(f, "Node problems:\n {:?}", self.node_problem_frequency)?;
		writeln!(f, "Pred problems:\n {:?}", self.pred_problem_frequency)?;
		writeln!(f, "\n--------------------------------------------------\n")?;
		
		for i in 0..self.samples.len() {
			write!(f, "Plan {i}")?;
			if i == self.chosen {
				write!(f, " (CHOSEN)")?;
			}
			writeln!(f, "")?;
			writeln!(f, "------------------")?;
			self.dump_plan(f, i)?;
		}
		
		Ok(())
	}
}

fn add_problem(problems: &mut HashMap<usize, Vec<NodeProblem>>, idx: &usize, problem: NodeProblem) {
	if let None = problems.get(idx) {
		problems.insert(*idx, Vec::new());
	}
	problems.get_mut(idx).unwrap().push(problem);
}

fn proc_plan(
	node: Arc<dyn ExecutionPlan>,
	node_idx: &mut usize,
	plan: &MeasuredPlan,
	problems: &mut HashMap<usize, Vec<NodeProblem>>
) {
	if let Ok(card) = plan.cardinalities[*node_idx] {
		let est_card = plan.plan.est_cards[*node_idx];
		let q = calculate_q_error(est_card, card);
		if CardQuality::from_q_err(q) == CardQuality::Poor {
			add_problem(problems, &node_idx, NodeProblem::CardinalityMisestimation(
				est_card as usize, card, q
			));
		}
	} else if let Err(MeasureError::Died) = plan.cardinalities[*node_idx] {
		add_problem(problems, &node_idx, NodeProblem::Crash);
	}
		
	for c in node.children() {
		*node_idx += 1;
		proc_plan(c.clone(), node_idx, plan, problems);
	}
}

fn get_pred_true_name(node: Arc<dyn PhysicalExpr>) -> String {
	let debug_repr = format!("{:?}", node);
	let name = debug_repr.split(' ').next().unwrap();
	match name {
		"BinaryExpr" => {
			let expr: &BinaryExpr = node.as_any().downcast_ref().unwrap();
			expr.op().to_string()
		},
		o => o.to_string(),
	}
}

fn get_pred_nodes_expr(node: Arc<dyn PhysicalExpr>, out: &mut Vec<String>) {
	out.push(get_pred_true_name(node.clone()));
	for c in node.children() {
		get_pred_nodes_expr(c.clone(), out);
	}
}

fn get_pred_nodes(node: Arc<dyn ExecutionPlan>) -> HashSet<String> {
	let mut expr_nodes = Vec::new();
	match node.name() {
		"FilterExec" => {
			let filter: &FilterExec = node.as_any().downcast_ref().unwrap();
			let pred = filter.predicate();
			println!("Filter with predicate {:?}", pred);
			get_pred_nodes_expr(pred.clone(), &mut expr_nodes);
			println!("Got nodes {:?}", expr_nodes);
		},
		"HashJoinExec" => {
			let join: &HashJoinExec = node.as_any().downcast_ref().unwrap();
			for (l, r) in &join.on {
				get_pred_nodes_expr(l.clone(), &mut expr_nodes);
				get_pred_nodes_expr(r.clone(), &mut expr_nodes);
			}
			if let Some(e) = &join.filter {
				get_pred_nodes_expr(e.expression().clone(), &mut expr_nodes);
			}
		},
		"NestedLoopJoinExec" => {
			let join: &NestedLoopJoinExec = node.as_any().downcast_ref().unwrap();
			if let Some(e) = &join.filter() {
				get_pred_nodes_expr(e.expression().clone(), &mut expr_nodes);
			}
		},
		_ => (),
	}
	expr_nodes.into_iter().collect()
}

fn collect_freq_info(
	plan_idx: usize,
	node_idx: &mut usize,
	node: Arc<dyn ExecutionPlan>,
	problems: &HashMap<usize, HashMap<usize, Vec<NodeProblem>>>,
	node_freqs: &mut HashMap<String, (usize, usize)>,
	pred_freqs: &mut HashMap<String, (usize, usize)>,
	root_only: bool,
) -> bool {
	// Do not process crashed nodes! 
	let just_crash = problems.get(&plan_idx)
		.and_then(|x| x.get(node_idx))
		.map(|x| x.len() == 1 && matches!(x[0], NodeProblem::Crash));
	if let Some(true) = just_crash {
		// There may be live nodes below this depending on if early stopping was enabled.
		// Code duplication here is annoying but not worth a refactor.
		for c in node.children() {
			*node_idx += 1;
			collect_freq_info(plan_idx, node_idx, c.clone(),
							  problems, node_freqs, pred_freqs, root_only);
		}
		return false;
	}	
	
	let mut has_problem = problems.get(&plan_idx)
		.and_then(|x| x.get(node_idx))
		.and_then(|x| x.iter().filter(|x| !matches!(x, NodeProblem::Crash)).next())
		.is_some();
	
	let mut bad_child = false;
	for c in node.children() {
		*node_idx += 1;
		bad_child = bad_child ||
			collect_freq_info(plan_idx, node_idx, c.clone(),
							  problems, node_freqs, pred_freqs, root_only);
	}
	let true_has_problem = has_problem;
	if root_only && bad_child {
		has_problem = false;
	}
	if let Some(entry) = node_freqs.get_mut(node.name()) {
		*entry = (entry.0 + if has_problem { 1 } else { 0 }, entry.1 + 1);
	} else {
		node_freqs.insert(node.name().to_string(), (if has_problem { 1 } else { 0 }, 1));
	}
	for pred_kind in get_pred_nodes(node.clone()) {
		if let Some(entry) = pred_freqs.get_mut(&pred_kind) {
			*entry = (entry.0 + if has_problem { 1 } else { 0 }, entry.1 + 1);
		} else {
			pred_freqs.insert(pred_kind, (if has_problem { 1 } else { 0 }, 1));
		}
	}
	return true_has_problem;
}

pub async fn analyze(
	benches: impl Stream<Item = BenchmarkOutput>,
	cfg: AnalysisConfig
) -> Report {
	let queries: Vec<QueryReport> = benches.then(|bench| async move {	
		let placement: Vec<_> = bench.plans.iter().enumerate()
			.sorted_by(|(_, x), (_, y)| x.plan.est_costs[0]
					   .partial_cmp(&y.plan.est_costs[0]).unwrap())
			.map(|(x, _)| x).collect();
		let mut est_ranks = vec![0; placement.len()];
		for i in &placement {
			est_ranks[placement[*i]] = *i;
		}
		let mut problems = HashMap::new();	
		
		for (i, plan) in bench.plans.iter().enumerate() {			
			println!("perf of plan {i}:\n {plan}");
			let mut plan_problems = HashMap::new();
			let mut idx = 0;
			proc_plan(plan.plan.tree.clone(), &mut idx, plan, &mut plan_problems);
			problems.insert(i, plan_problems);
		}

		for (i, plan) in bench.plans.iter().enumerate() {
			let sz = plan.plan.size();
			let plan_problems = problems.get_mut(&i).unwrap();
			for n_i in 0..sz {
				if let Ok(runtime) = &plan.sub_runtimes.as_ref().unwrap()[n_i] {
					let mut est_rank = 0;
					let mut real_rank = 0;
					for (j, oplan) in bench.plans.iter().enumerate() {
						if i == j { continue }
						if partial_eq_plans(plan.plan.tree.clone(), oplan.plan.tree.clone(), n_i) {
							if let Ok(oruntime) = &oplan.sub_runtimes.as_ref().unwrap()[n_i] {
								if oplan.plan.est_costs[n_i] < plan.plan.est_costs[n_i] {
									est_rank += 1;
								}
								if oruntime.mean.as_millis() < runtime.mean.as_millis() {
									real_rank += 1;
								}
							}
						}
					}
					if est_rank != real_rank {
						add_problem(plan_problems, &n_i, NodeProblem::CostMisestimation(
							runtime.mean.as_millis(),
							plan.plan.est_costs[n_i],
							est_rank,
							real_rank,
						));
					}
				}
			}
		}

		let mut node_freq = HashMap::new();
		let mut pred_freq = HashMap::new();
		for (i, plan) in bench.plans.iter().enumerate() {
			let mut node_idx = 0;
			collect_freq_info(i, &mut node_idx, plan.plan.tree.clone(),
							  &problems, &mut node_freq, &mut pred_freq,
							  cfg.root_problems_only);
		}
		
		QueryReport {
			node_problem_frequency: node_freq,
			pred_problem_frequency: pred_freq,
			optimal_chosen: bench.chosen_idx == 0,
			name: bench.name,
			metrics: bench.metrics,
			samples: bench.plans,
			chosen: bench.chosen_idx,
			node_problems: problems,
		}
	}).collect().await;

	Report {
		opt_freq: queries
			.iter()
			.map(|q| if q.optimal_chosen { 1.0 } else { 0.0 })
			.sum::<f64>() / queries.len() as f64,
		avg_taqo_score: queries
			.iter()
			.map(|q| q.metrics.taqo_score_s)
			.sum::<f64>() / queries.len() as f64,
		avg_taqo_acc: queries
			.iter()
			.map(|q| q.metrics.taqo_accuracy_percent)
			.sum::<f64>() / queries.len() as f64,
		avg_perf_factor: queries
			.iter()
			.map(|q| q.metrics.performance_factor)
			.sum::<f64>() / queries.len() as f64,
		queries,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{benchmark::{CardQuality, RuntimeStats}, common::Plan};
	use datafusion::physical_plan::empty::EmptyExec;
	use datafusion::arrow::datatypes::{Field, Schema};
	use std::time::Duration;
	use std::sync::Arc;

	// Helper function to create a test plan
	fn create_test_plan() -> Arc<dyn ExecutionPlan> {
		let schema = Arc::new(Schema::new(Vec::<Field>::new()));
		Arc::new(EmptyExec::new(schema))
	}

	// Helper function to create a test MeasuredPlan
	fn create_measured_plan(
		runtime_ms: u64, est_cost: f64, est_card: f64, act_card: usize
	) -> MeasuredPlan {
		let plan_tree = create_test_plan();
		MeasuredPlan {
			plan: Plan::new(plan_tree, vec![est_cost], vec![est_card]),
			runtime: Ok(RuntimeStats::from_duration(Duration::from_millis(runtime_ms))),
			cardinalities: vec![Ok(act_card)],
			sub_runtimes: None,
		}
	}
	
	// Helper function to create a measured plan with issues
	fn create_problematic_plan(error_type: MeasureError) -> MeasuredPlan {
		let plan_tree = create_test_plan();
		MeasuredPlan {
			plan: Plan::new(plan_tree, vec![100.0], vec![1000.0]),
			runtime: Err(error_type),
			cardinalities: vec![Err(error_type)],
			sub_runtimes: None,
		}
	}

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
	fn test_node_problems() {
		// Test creating and processing node problems
		let mut problems: HashMap<usize, Vec<NodeProblem>> = HashMap::new();
		
		// Test CardinalityMisestimation problem
		add_problem(&mut problems, &0, NodeProblem::CardinalityMisestimation(1000, 100, 10.0));
		assert_eq!(problems.len(), 1);
		
		// Get a reference to verify
		let node0_problems = problems.get(&0).unwrap();
		assert_eq!(node0_problems.len(), 1);
		
		// Add another problem to the same node
		add_problem(&mut problems, &0, NodeProblem::Crash);
		assert_eq!(problems.len(), 1);
		
		// Get a reference to verify
		let node0_problems = problems.get(&0).unwrap();
		assert_eq!(node0_problems.len(), 2);
		
		// Add problem to a different node
		add_problem(&mut problems, &1, NodeProblem::CostMisestimation(200, 10.0, 100, 500));
		assert_eq!(problems.len(), 2);
		
		// Get references to verify
		let node0_problems = problems.get(&0).unwrap();
		let node1_problems = problems.get(&1).unwrap();
		assert_eq!(node1_problems.len(), 1);
		
		// Verify problem details
		match &node0_problems[0] {
			NodeProblem::CardinalityMisestimation(est, act, q_err) => {
				assert_eq!(*est, 1000);
				assert_eq!(*act, 100);
				assert_eq!(*q_err, 10.0);
			},
			_ => panic!("Wrong problem type"),
		}
		
		match &node0_problems[1] {
			NodeProblem::Crash => {},
			_ => panic!("Wrong problem type"),
		}
		
		match &node1_problems[0] {
			NodeProblem::CostMisestimation(runtime_us, est_cost, est_card, act_card) => {
				assert_eq!(*runtime_us, 200);
				assert_eq!(*est_cost, 10.0);
				assert_eq!(*est_card, 100);
				assert_eq!(*act_card, 500);
			},
			_ => panic!("Wrong problem type"),
		}
	}
	
	#[test]
	fn test_query_report() {
		// Create measured plans
		let plan1 = create_measured_plan(100, 5.0, 100.0, 90); // Good plan, slight underestimation
		let plan2 = create_measured_plan(200, 15.0, 500.0, 50); // Worse plan, large overestimation
		let plan3 = create_problematic_plan(MeasureError::Died); // Failed plan
		
		// Create node problems
		let mut node_problems: HashMap<usize, HashMap<usize, Vec<NodeProblem>>> = HashMap::new();
		let mut plan0_problems: HashMap<usize, Vec<NodeProblem>> = HashMap::new();
		add_problem(&mut plan0_problems, &0, NodeProblem::CardinalityMisestimation(100, 90, 1.11));
		node_problems.insert(0, plan0_problems);
		
		let mut plan1_problems: HashMap<usize, Vec<NodeProblem>> = HashMap::new();
		add_problem(&mut plan1_problems, &0, NodeProblem::CardinalityMisestimation(500, 50, 10.0));
		node_problems.insert(1, plan1_problems);
		
		let mut plan2_problems: HashMap<usize, Vec<NodeProblem>> = HashMap::new();
		add_problem(&mut plan2_problems, &0, NodeProblem::Crash);
		node_problems.insert(2, plan2_problems);
		
		// Create query report
		let query_report = QueryReport {
			optimal_chosen: true,
			name: "test_query".to_string(),
			metrics: OptimizerMetrics {
				taqo_score_s: 0.2,
				taqo_accuracy_percent: 80.0,
				performance_factor: 1.0,
				avg_q_error: 1.11,
			},
			samples: vec![plan1, plan2, plan3],
			chosen: 0,
			node_problems,
		};
		
		// Check display formatting works
		let report_str = format!("{}", query_report);
		assert!(report_str.contains("test_query"));
		assert!(report_str.contains("TAQO Score"));
		assert!(report_str.contains("Cardinality estimation is excellent"));
	}
	
	#[test]
	fn test_report() {
		// Create a basic report
		let query1 = QueryReport {
			node_problem_frequency: HashMap::new(),
			pred_problem_frequency: HashMap::new(),
			optimal_chosen: true,
			name: "query1".to_string(),
			metrics: OptimizerMetrics {
				taqo_score_s: 0.2,
				taqo_accuracy_percent: 90.0,
				performance_factor: 1.0,
				avg_q_error: 1.5,
			},
			samples: vec![create_measured_plan(100, 5.0, 100.0, 90)],
			chosen: 0,
			node_problems: HashMap::new(),
		};
		
		let query2 = QueryReport {
			node_problem_frequency: HashMap::new(),
			pred_problem_frequency: HashMap::new(),
			optimal_chosen: false,
			name: "query2".to_string(),
			metrics: OptimizerMetrics {
				taqo_score_s: 0.5,
				taqo_accuracy_percent: 70.0,
				performance_factor: 0.5,
				avg_q_error: 5.0,
			},
			samples: vec![
				create_measured_plan(200, 10.0, 200.0, 50),
				create_measured_plan(100, 5.0, 100.0, 90)
			],
			chosen: 0,
			node_problems: HashMap::new(),
		};
		
		let report = Report {
			opt_freq: 0.5, // 1 out of 2 queries chose optimal plan
			avg_taqo_score: 0.35, // (0.2 + 0.5) / 2
			avg_taqo_acc: 80.0, // (90 + 70) / 2
			avg_perf_factor: 0.75, // (1.0 + 0.5) / 2
			queries: vec![query1, query2],
		};
		
		// Check display formatting works
		let report_str = format!("{}", report);
		assert!(report_str.contains("GLOBAL STATS"));
		assert!(report_str.contains("Optimality Frequency (OF): 50.00"));
		assert!(report_str.contains("Average TAQO Score"));
		assert!(report_str.contains("Average TAQO Accuracy"));
		assert!(report_str.contains("query1"));
		assert!(report_str.contains("query2"));
	}
}
	
	

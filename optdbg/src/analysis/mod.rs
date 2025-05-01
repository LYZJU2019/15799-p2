use std::sync::Arc;

use datafusion::{common::HashMap, physical_plan::ExecutionPlan};
use itertools::Itertools;

use crate::{benchmark::{calculate_q_error, BenchmarkOutput, CardQuality, MeasuredPlan, OptimizerMetrics}, common::{partial_eq_plans, MeasureError}};

// Placeholder type.
pub struct AnalysisConfig;	

#[derive(Debug)]
pub enum NodeProblem {
	Crash,
	CardinalityMisestimation(usize, usize, f64),
	CostMisestimation(f64, usize, usize),
}

pub struct Report {
	pub metrics: OptimizerMetrics,
	pub samples: Vec<MeasuredPlan>,
	/// Index of chosen plan in `samples` field.
	pub chosen: usize,
	pub node_problems: HashMap<usize, HashMap<usize, Vec<NodeProblem>>>
}

impl Report {
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
		writeln!(f, "Sampled {} plans.", self.samples.len())?;
		writeln!(f, "TAQO Score (s): {:.4} (lower is better)", self.metrics.taqo_score_s)?;
		writeln!(f, "TAQO Accuracy: {:.2}% (higher is better)", self.metrics.taqo_accuracy_percent)?;
		writeln!(f, "Performance Factor (PF): {:.2}%", self.metrics.performance_factor * 100.0)?;
		writeln!(f, "Average Q-Error: {:.2} (closer to 1.0 is better)", self.metrics.avg_q_error)?;
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
		writeln!(f, "====================================================\n")?;
		
		for i in 0..self.samples.len() {
			writeln!(f, "Plan {i}")?;
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

pub fn analyze(bench: BenchmarkOutput, _cfg: AnalysisConfig) -> Report {
	let placement: Vec<_> = bench.plans.iter().enumerate()
		.sorted_by(|(_, x), (_, y)| x.plan.est_costs[0].partial_cmp(&y.plan.est_costs[0]).unwrap())
		.map(|(x, _)| x).collect();
	let mut est_ranks = vec![0; placement.len()];
	for i in &placement {
		est_ranks[placement[*i]] = *i;
	}
	let mut problems = HashMap::new();
	for (i, plan) in bench.plans.iter().enumerate() {
		let mut plan_problems = HashMap::new();
		let mut idx = 0;
		proc_plan(plan.plan.tree.clone(), &mut idx, plan, &mut plan_problems);
		problems.insert(i, plan_problems);
	}

	for (i, plan) in bench.plans.iter().enumerate() {
		let sz = plan.plan.size();
		let plan_problems = problems.get_mut(&i).unwrap();
		for n_i in 0..sz {
			let Ok(runtime) = plan.sub_runtimes.as_ref().unwrap()[n_i] else {
				continue
			};
			let mut est_rank = 0;
			let mut real_rank = 0;
			for (j, oplan) in bench.plans.iter().enumerate() {
				if i == j { continue }
				if partial_eq_plans(plan.plan.tree.clone(), oplan.plan.tree.clone(), n_i) {
					let Ok(oruntime) = oplan.sub_runtimes.as_ref().unwrap()[n_i] else {
						continue
					};
					if oplan.plan.est_costs[n_i] < plan.plan.est_costs[n_i] {
						est_rank += 1;
					}
					if oruntime < runtime
					{
						real_rank += 1;
					}
				}
			}
			if est_rank != real_rank {
				add_problem(plan_problems, &n_i, NodeProblem::CostMisestimation(
					plan.plan.est_costs[n_i],
					est_rank,
					real_rank,
				));
			}
		}
	}
	
	Report {
		metrics: bench.metrics,
		samples: bench.plans,
		chosen: bench.chosen_idx,
		node_problems: problems,
	}
}
	

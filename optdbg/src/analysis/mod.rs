use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;

use crate::{benchmark::{calculate_q_error, BenchmarkOutput, CardQuality, MeasuredPlan}, common::MeasureError};

// Placeholder type.
pub struct AnalysisConfig;	

pub enum Problem {
	Crash(usize, usize),
	CardinalityMisestimation(usize, usize, usize, usize, f64),
	// CostMisestimation(usize, usize),
}

// Placeholder type.
pub struct Report {
	samples: Vec<MeasuredPlan>,
	/// Index of chosen plan in `samples` field.
	chosen: usize,
	problems: Vec<Problem>
}

impl std::fmt::Display for Report {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

fn partial_eq

fn proc_plan(
	node: Arc<dyn ExecutionPlan>,
	node_idx: &mut usize,
	plan: &MeasuredPlan,
	plan_idx: usize,
	problems: &mut Vec<Problem>
) {
	if let Ok(card) = plan.cardinalities[*node_idx] {
		let est_card = plan.plan.est_cards[*node_idx];
		let q = calculate_q_error(est_card, card);
		if CardQuality::from_q_err(q) == CardQuality::Poor {
			problems.push(Problem::CardinalityMisestimation(
				plan_idx, *node_idx, est_card as usize, card, q
			));
		}
	} else if let Err(MeasureError::Died) = plan.cardinalities[*node_idx] {
		problems.push(Problem::Crash(plan_idx, *node_idx));
	}
		
	for c in node.children() {
		*node_idx += 1;
		proc_plan(c.clone(), node_idx, plan, plan_idx, problems);
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
	let mut problems = Vec::new();
	for (i, plan) in bench.plans.iter().enumerate() {
		let mut idx = 0;
		proc_plan(plan.plan.tree.clone(), &mut idx, plan, i, &mut problems);
	}
	
	Report {
		samples: bench.plans,
		chosen: bench.chosen_idx,
		problems, 
	}
}
	

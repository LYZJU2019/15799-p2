use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};

// TODO find a way to compare plan properties?
fn eq_plans(a: Arc<dyn ExecutionPlan>, b: Arc<dyn ExecutionPlan>) -> bool {
	if a.name() != b.name() {
		return false;
	}
	let a_childs = a.children();
	let b_childs = b.children();
	if a_childs.len() != b_childs.len() {
		return false;
	}
	for (i,j) in a_childs.into_iter().zip(b_childs.into_iter()) {
		if !eq_plans(i.clone(), j.clone()) {
			return false;
		}
	}
	return true;
}

fn format_plan(
	f: &mut std::fmt::Formatter<'_>,
	plan: Arc<dyn ExecutionPlan>,
	indent_level: usize
) -> std::fmt::Result {
	for _ in 0..indent_level {
		write!(f, "  ")?;
	}
	writeln!(f, "{}", plan.name())?;
	for child in plan.children() {
		format_plan(f, child.clone(), indent_level + 1)?;
	}
	Ok(())
}

#[derive(Clone)]
pub struct Plan {
	pub tree: Arc<dyn ExecutionPlan>,
	pub est_cost: f64,
}

impl Plan {
	pub fn new(tree: Arc<dyn ExecutionPlan>, est_cost: f64) -> Self {
		Self { tree, est_cost } 
	}
}

impl std::cmp::PartialEq for Plan {
	fn eq(&self, other: &Self) -> bool {
		eq_plans(self.tree.clone(), other.tree.clone()) && self.est_cost == other.est_cost
	}
}

impl std::cmp::Eq for Plan {}

impl std::fmt::Display for Plan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		format_plan(f, self.tree.clone(), 0)
	}
}

#[derive(Serialize, Deserialize)]
pub struct PlanMeasurements {
	pub cardinalities: Vec<Option<usize>>,
	pub sub_runtimes: Option<Vec<Option<Duration>>>,
}

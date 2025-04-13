use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;
use thiserror::Error;
use serde::{Deserialize, Serialize};

// TODO find a way to compare plan properties?
/// Returns true if two plans have same structure.
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

/// Returns the number of nodes in a plan.
fn plan_size(a: Arc<dyn ExecutionPlan>) -> usize {
	1 + a.children().into_iter().cloned().map(plan_size).sum::<usize>()
}

/// Displays the name of each node in a plan with tree-style indentation.
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

/// Main wrapper type of a physical plan alongside estimated cost.
#[derive(Clone)]
pub struct Plan {
	pub tree: Arc<dyn ExecutionPlan>,
	pub est_cost: f64,
}

impl Plan {
	pub fn new(tree: Arc<dyn ExecutionPlan>, est_cost: f64) -> Self {
		Self { tree, est_cost } 
	}

	/// Get the number of nodes in a pln.
	pub fn size(&self) -> usize {
		plan_size(self.tree.clone())
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

#[derive(Clone, Copy, Error, Debug, PartialEq, Serialize, Deserialize)]
pub enum MeasureError {
	#[error("exhausted memory")]
	OOM,
	#[error("timed out")]
	Timeout,
}

/// Wrapper type for easier IPC. See `MeasuredPlan`.
#[derive(Serialize, Deserialize)]
pub struct PlanMeasurements {
	pub cardinalities: Vec<Result<usize, MeasureError>>,	
	pub sub_runtimes: Option<Vec<Result<Duration, MeasureError>>>,
}

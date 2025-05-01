use std::time::Duration;
use std::{hash::Hash, sync::Arc};

use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// TODO find a way to compare plan properties?
/// Returns true if two plans have same structure.
fn eq_plans(a: Arc<dyn ExecutionPlan>, b: Arc<dyn ExecutionPlan>) -> bool {
    if a.name() != b.name() {
        return false;
    }

    if a.as_any().type_id() != b.as_any().type_id() {
        return false;
    }

    if !eq_properties(a.properties(), b.properties()) {
        return false;
    }

    let a_childs = a.children();
    let b_childs = b.children();
    if a_childs.len() != b_childs.len() {
        return false;
    }
    for (i, j) in a_childs.into_iter().zip(b_childs.into_iter()) {
        if !eq_plans(i.clone(), j.clone()) {
            return false;
        }
    }
    true
}

fn partial_eq_plans_help(
	a: Arc<dyn ExecutionPlan>,
	b: Arc<dyn ExecutionPlan>,
	cur: &mut usize,
	target_i: usize
) -> bool {
	if *cur == target_i {
		return true;
	}	
    if a.name() != b.name() {
        return false;
    }

    if a.as_any().type_id() != b.as_any().type_id() {
        return false;
    }

    let a_childs = a.children();
    let b_childs = b.children();
    if a_childs.len() != b_childs.len() {
        return false;
    }
    for (i, j) in a_childs.into_iter().zip(b_childs.into_iter()) {		
		*cur += 1;
        if !partial_eq_plans_help(i.clone(), j.clone(), cur, target_i) {
            return false;
        }
    }
    true
}

/// Compares plan `a` to `b`, assuming that the node at preorder index `i` of `a`
/// is equal to any node of b at the same position.
pub fn partial_eq_plans(
	a: Arc<dyn ExecutionPlan>,
	b: Arc<dyn ExecutionPlan>,
	target_i: usize
) -> bool {
	let mut cur = 0;
	partial_eq_plans_help(a, b, &mut cur, target_i)
}

// TODO: not a complete comparison
fn eq_properties(prop_a: &PlanProperties, prop_b: &PlanProperties) -> bool {
    prop_a.boundedness == prop_b.boundedness
        && prop_a.emission_type == prop_b.emission_type
        && prop_a.partitioning == prop_b.partitioning
        && prop_a.output_ordering() == prop_b.output_ordering()
}

impl Hash for Plan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        format!("{:?}", self.tree).hash(state);
    }
}

/// Returns the number of nodes in a plan.
fn plan_size(a: Arc<dyn ExecutionPlan>) -> usize {
    1 + a
        .children()
        .into_iter()
        .cloned()
        .map(plan_size)
        .sum::<usize>()
}

/// Displays the name of each node in a plan with tree-style indentation.
pub fn dump_plan(
    plan: Arc<dyn ExecutionPlan>,
    indent_level: usize,
) {
    for _ in 0..indent_level {
        print!("  ");
    }
	println!("{}", plan.name());
    for child in plan.children() {
        dump_plan(child.clone(), indent_level + 1);
    }
}

// generalize at some point.....
/// Displays the name of each node in a plan with tree-style indentation.
pub fn format_plan_with_2preorder_help<T1: std::fmt::Display, T2: std::fmt::Display>(
    f: &mut std::fmt::Formatter<'_>,
    plan: Arc<dyn ExecutionPlan>,
    indent_level: usize,
    preorder_data1: &Vec<T1>,
    preorder_data2: &Vec<T2>,
    index: &mut usize,
) -> std::fmt::Result {
    for _ in 0..indent_level {
        write!(f, "  ")?;
    }
    writeln!(
        f,
        "{} ({}, {})",
        plan.name(),
        preorder_data1[*index],
        preorder_data2[*index]
    )?;
    *index += 1;
    for child in plan.children() {
        format_plan_with_2preorder_help(
            f,
            child.clone(),
            indent_level + 1,
            preorder_data1,
            preorder_data2,
            index,
        )?;
    }
    Ok(())
}

/// Main wrapper type of a physical plan alongside estimated cost.
#[derive(Clone, Debug)]
pub struct Plan {
    pub tree: Arc<dyn ExecutionPlan>,
    /// Preorder array of subplan estimated costs.
    pub est_costs: Vec<f64>,
    /// Preorder array of subplan estimated cardinalities.
    pub est_cards: Vec<f64>,
}

impl Plan {
    pub fn new(tree: Arc<dyn ExecutionPlan>, est_costs: Vec<f64>, est_cards: Vec<f64>) -> Self {
        Self {
            tree,
            est_costs,
            est_cards,
        }
    }

    /// Get the number of nodes in a pln.
    pub fn size(&self) -> usize {
        plan_size(self.tree.clone())
    }
}

impl std::cmp::PartialEq for Plan {
    fn eq(&self, other: &Self) -> bool {
        eq_plans(self.tree.clone(), other.tree.clone())
    }
}

impl std::cmp::Eq for Plan {}

impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut i = 0;
        format_plan_with_2preorder_help(
            f,
            self.tree.clone(),
            0,
            &self.est_costs,
            &self.est_cards,
            &mut i,
        )
    }
}

#[derive(Clone, Copy, Error, Debug, PartialEq, Serialize, Deserialize)]
pub enum MeasureError {
    #[error("query failed")]
    Died,
    #[error("timed out")]
    Timeout,
}

/// Wrapper type for easier IPC. See `MeasuredPlan`.
#[derive(Serialize, Deserialize)]
pub struct PlanMeasurements {
    pub cardinalities: Vec<Result<usize, MeasureError>>,
    pub sub_runtimes: Option<Vec<Result<Duration, MeasureError>>>,
}

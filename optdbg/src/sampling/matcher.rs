use optd_og_core::{
    nodes::{ArcPlanNode, NodeType, PlanNodeOrGroup},
    optimizer::Optimizer,
    rules::{Rule, RuleMatcher},
};

fn matcher_matches_node<T: NodeType>(matcher: &RuleMatcher<T>, node: &ArcPlanNode<T>) -> bool {
    let plan = node.as_ref(); // Deref Arc

    match matcher {
        RuleMatcher::Any => true,
        RuleMatcher::AnyMany => true,
        RuleMatcher::MatchNode {
            typ,
            children: matchers,
        } => {
            if &plan.typ != typ {
                return false;
            }

            let plan_children: Vec<_> = plan
                .children
                .iter()
                .filter_map(|child| match child {
                    PlanNodeOrGroup::PlanNode(p) => Some(p),
                    _ => None, // Skip unresolved groups
                })
                .collect();

            if plan_children.len() != matchers.len() {
                return false;
            }

            for (matcher, child) in matchers.iter().zip(plan_children.iter()) {
                if !matcher_matches_node(matcher, child) {
                    return false;
                }
            }

            true
        }

        RuleMatcher::MatchDiscriminant {
            typ_discriminant,
            children: matchers,
        } => {
            if std::mem::discriminant(&plan.typ) != *typ_discriminant {
                return false;
            }

            let plan_children: Vec<_> = plan
                .children
                .iter()
                .filter_map(|child| match child {
                    PlanNodeOrGroup::PlanNode(p) => Some(p),
                    _ => None,
                })
                .collect();

            if plan_children.len() != matchers.len() {
                return false;
            }

            for (matcher, child) in matchers.iter().zip(plan_children.iter()) {
                if !matcher_matches_node(matcher, child) {
                    return false;
                }
            }

            true
        }
    }
}

pub fn is_rule_applicable<T, O>(rule: &dyn Rule<T, O>, root: &ArcPlanNode<T>) -> bool
where
    T: NodeType,
    O: Optimizer<T> + 'static,
{
    let matcher = rule.matcher();
    let mut stack = vec![root.clone()];

    while let Some(node) = stack.pop() {
        if matcher_matches_node(matcher, &node) {
            return true;
        }

        let plan = node.as_ref();
        for child in &plan.children {
            if let PlanNodeOrGroup::PlanNode(c) = child {
                stack.push(c.clone());
            }
        }
    }

    false
}

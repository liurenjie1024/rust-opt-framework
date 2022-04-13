



use anyhow::{ensure};




use crate::error::OptResult;
use crate::heuristic::binding::Binding;
use crate::heuristic::graph::{HepOptimizerNode, PlanGraph};
use crate::heuristic::HepNodeId;
use crate::optimizer::{OptExpr, Optimizer, OptimizerContext};
use crate::plan::Plan;

use crate::rules::{Rule, RuleImpl, RuleResult};


/// Match order of plan tree.
#[derive(Copy, Clone)]
pub enum MatchOrder {
    BottomUp,
    TopDown,
}

pub struct HepOptimizer {
    match_order: MatchOrder,
    /// Max number of iteration
    max_iter_times: usize,
    rules: Vec<RuleImpl>,
    pub(super) graph: PlanGraph,
    context: OptimizerContext,
}

impl Optimizer for HepOptimizer {
    type Expr = HepOptimizerNode;
    type ExprHandle = HepNodeId;
    type Group = HepOptimizerNode;
    type GroupHandle = HepNodeId;

    fn context(&self) -> &OptimizerContext {
        &self.context
    }

    fn group_at(&self, group_handle: HepNodeId) -> &HepOptimizerNode {
        &self.graph.graph[group_handle]
    }

    fn expr_at(&self, expr_handle: HepNodeId) -> &HepOptimizerNode {
        &self.graph.graph[expr_handle]
    }

    fn find_best_plan(mut self) -> OptResult<Plan> {
        for _times in 0..self.max_iter_times {
            // The plan no longer changes after iteration
            let mut fixed_point = true;
            let node_ids = self.graph.nodes_iter(self.match_order.clone());
            for node_id in node_ids {
                let expr_handle = node_id;

                for rule in &*self.rules.clone() {
                    println!(
                        "Trying to apply rule {:?} to expression {:?}",
                        rule,
                        self.expr_at(expr_handle).operator()
                    );
                    if self.apply_rule(rule.clone(), expr_handle.clone())? {
                        println!(
                            "Plan after applying rule {:?} is {:?}",
                            rule,
                            self.graph.to_plan()
                        );
                        fixed_point = false;
                        break;
                    } else {
                        println!(
                            "Skipped applying rule {:?} to expression {:?}",
                            rule,
                            self.expr_at(expr_handle).operator()
                        );
                    }
                }

                if !fixed_point {
                    break;
                }
            }

            if fixed_point {
                break;
            }
        }

        Ok(self.graph.to_plan())
    }
}

impl HepOptimizer {
    pub fn new(
        match_order: MatchOrder,
        max_iter_times: usize,
        rules: Vec<RuleImpl>,
        plan: Plan,
        context: OptimizerContext,
    ) -> Self {
        Self {
            match_order,
            max_iter_times,
            rules,
            graph: PlanGraph::from(plan),
            context,
        }
    }

    fn apply_rule(&mut self, rule: RuleImpl, expr_handle: HepNodeId) -> OptResult<bool> {
        let original_hep_node_id = expr_handle;
        if let Some(opt_node) = Binding::new(expr_handle, &*rule.pattern(), self).next() {
            let mut results = RuleResult::new();
            rule.apply(opt_node, self, &mut results)?;

            for (idx, new_expr) in results.results().enumerate() {
                ensure!(
                    idx < 1,
                    "Rewrite rule should not return no more than 1 result."
                );
                return Ok(self
                    .graph
                    .replace_opt_expression(new_expr, original_hep_node_id));
            }

            // No transformation generated.
            return Ok(false);
        } else {
            Ok(false)
        }
    }
}

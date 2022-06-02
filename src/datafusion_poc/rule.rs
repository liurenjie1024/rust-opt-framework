use std::rc::Rc;
use datafusion::common::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
use crate::heuristic::{HepOptimizer, MatchOrder};
use crate::optimizer::{Optimizer, OptimizerContext};
use crate::plan::{Plan, PlanNode};
use crate::rules::RuleImpl;

/// An adapter converts [`HeuristicOptimizer`] into datafusion's optimizer rule.
///
/// It works as followings:
/// ```no
/// Datafusion logical plan -> Our logical plan -> Heuristic optimizer -> Our logical plan ->
/// Datafusion logical plan
/// ```
pub struct DFOptimizerAdapterRule {
  /// Our rules
  rules: Vec<RuleImpl>,
}

impl OptimizerRule for DFOptimizerAdapterRule {
  fn optimize(&self, df_plan: &LogicalPlan, _execution_props: &ExecutionProps) ->
  datafusion::common::Result<LogicalPlan> {
    println!("Beginning to execute heuristic optimizer");
    let plan = Plan::new(Rc::new(PlanNode::try_from(df_plan)
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?
    ));

    // Construct heuristic optimizer here
    let hep_optimizer = HepOptimizer::new(MatchOrder::TopDown, 1000, self.rules.clone(), plan,
                                          OptimizerContext {});
    let optimized_plan = hep_optimizer.find_best_plan()
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

    LogicalPlan::try_from(&*optimized_plan.root())
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))
  }

  fn name(&self) -> &str {
    "DFOptimizerAdapterRule"
  }
}



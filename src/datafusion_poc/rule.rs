use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerRule;
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
  _rules: Vec<RuleImpl>,
}

impl OptimizerRule for DFOptimizerAdapterRule {
  fn optimize(&self, _plan: &LogicalPlan, _execution_props: &ExecutionProps) ->
                                                                            datafusion::common::Result<LogicalPlan> {
    todo!()
  }

  fn name(&self) -> &str {
    todo!()
  }
}



use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use crate::cascades::CascadesOptimizer;
use crate::datafusion_poc::plan::plan_node_to_df_physical_plan;
use crate::optimizer::{Optimizer, OptimizerContext};
use crate::plan::{Plan, PlanNode};
use crate::properties::PhysicalPropertySet;
use crate::rules::RuleImpl;
use async_trait::async_trait;


/// A query planner converting logical plan to physical plan.
///
/// It works as following:
/// ```no
/// Datafusion logical plan -> Our logical plan -> CBO -> Our physical plan -> Datafusion
/// physical plan
/// ```
pub struct DFQueryPlanner {
  rules: Vec<RuleImpl>,
  optimizer_ctx: OptimizerContext,
}

#[async_trait]
impl QueryPlanner for DFQueryPlanner {
  async fn create_physical_plan(&self, df_logical_plan: &LogicalPlan, session_state: &SessionState) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    println!("Beginning to execute heuristic optimizer");
    let logical_plan = Plan::new(Arc::new(PlanNode::try_from(df_logical_plan)
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?
    ));

    let optimizer = CascadesOptimizer::new(
      PhysicalPropertySet::default(),
      self.rules.clone(),
      logical_plan,
      self.optimizer_ctx.clone(),
    );

    let physical_plan = optimizer.find_best_plan()
        .map_err(|e| DataFusionError::Plan(format!("{:?}", e)))?;

    plan_node_to_df_physical_plan(&*physical_plan.root(),
                                  session_state,
                                  &self.optimizer_ctx,
    ).await.map_err(|e| DataFusionError::Plan(format!("Physical planner error: {:?}", e)))
  }
}

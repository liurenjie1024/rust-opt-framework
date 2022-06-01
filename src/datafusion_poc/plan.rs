use std::rc::Rc;
use anyhow::bail;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{and, LogicalPlan};
use crate::error::OptResult;
use crate::Expr;
use crate::Expr::Column;
use crate::operator::{Join, Limit, LogicalOperator, Projection, TableScan};
use crate::operator::Operator::Logical;
use crate::plan::{PlanNode, PlanNodeIdGen};

/// Convert data fusion logical plan to our plan.
impl TryFrom<LogicalPlan> for PlanNode {
  type Error = anyhow::Error;

  fn try_from(value: LogicalPlan) -> Result<Self, anyhow::Error> {
    let mut plan_node_id_gen = PlanNodeIdGen::new();
    df_logical_plan_to_plan_node(&value, &mut plan_node_id_gen)
  }
}

fn df_logical_plan_to_plan_node(df_plan: &LogicalPlan, id_gen: &mut PlanNodeIdGen) ->
OptResult<PlanNode> {
  let id = id_gen.next();
  let (operator, inputs) = match df_plan {
    LogicalPlan::Projection(projection) => {
      let operator = LogicalOperator::LogicalProjection(Projection::new(projection.expr.clone()));
      let inputs = vec![df_logical_plan_to_plan_node(&projection.input, id_gen)?];
      (operator, inputs)
    }
    LogicalPlan::Limit(limit) => {
      let operator = LogicalOperator::LogicalLimit(Limit::new(limit.n));
      let inputs = vec![df_logical_plan_to_plan_node(&limit.input, id_gen)?];
      (operator, inputs)
    }
    LogicalPlan::Join(join) => {
      let join_cond = join.on.iter()
          .map(|(left, right)| Column(left.clone()).eq(Column(right.clone())))
          .reduce(|a, b| and(a, b))
          .unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));
      let operator = LogicalOperator::LogicalJoin(Join::new(join.join_type, join_cond));
      let inputs = vec![df_logical_plan_to_plan_node(&join.left, id_gen)?,
                        df_logical_plan_to_plan_node(&join.right, id_gen)?,
      ];
      (operator, inputs)
    }
    LogicalPlan::TableScan(scan) => {
      let operator = LogicalOperator::LogicalScan(TableScan::new(&scan.table_name));
      let inputs = vec![];
      (operator, inputs)
    }
    plan => {
      bail!("Unsupported datafusion logical plan: {:?}", plan);
    }
  };

  Ok(PlanNode::new(id, Logical(operator), inputs.into_iter().map(Rc::new).collect()))
}

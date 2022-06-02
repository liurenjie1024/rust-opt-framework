use std::rc::Rc;
use std::sync::Arc;
use anyhow::bail;
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::{and, LogicalPlan};
use datafusion::logical_plan::JoinConstraint;
use crate::error::OptResult;
use crate::Expr;
use crate::Expr::{Column as ExprColumn};
use crate::operator::{Join, Limit, LogicalOperator, Projection, TableScan};
use crate::operator::LogicalOperator::{LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan};
use crate::operator::Operator::Logical;
use crate::plan::{PlanNode, PlanNodeIdGen};
use datafusion::logical_plan::plan::{Projection as DFProjection, Limit as DFLimit, Join as
DFJoin, TableScan as DFTableScan, DefaultTableSource};
use datafusion::logical_plan::{Operator as DFOperator};

/// Convert data fusion logical plan to our plan.
impl<'a> TryFrom<&'a LogicalPlan> for PlanNode {
  type Error = anyhow::Error;

  fn try_from(value: &'a LogicalPlan) -> Result<Self, anyhow::Error> {
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
          .map(|(left, right)| ExprColumn(left.clone()).eq(ExprColumn(right.clone())))
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

/// Converting logical plan to df plan.
impl<'a> TryFrom<&'a PlanNode> for LogicalPlan {
  type Error = anyhow::Error;

  fn try_from(plan_node: &'a PlanNode) -> OptResult<Self> {
    plan_node_to_df_logical_plan(plan_node)
  }
}

fn expr_to_df_join_condition(expr: &Expr) -> OptResult<Vec<(Column, Column)>> {
  match expr {
    Expr::BinaryExpr {
      left,
      op,
      right
    } if matches!(op, DFOperator::Eq) => {
      match (&**left, &**right) {
        (ExprColumn(left_col), ExprColumn(right_col)) => Ok(vec![(left_col.clone(), right_col
            .clone()
        )]),
        _ => bail!("Unsupported join condition to convert to datafusion join condition: {:?}",
          expr)
      }
    }
    _ => bail!("Unsupported join condition to convert to datafusion join condition: {:?}",
          expr)
  }
}

fn plan_node_to_df_logical_plan(plan_node: &PlanNode) -> OptResult<LogicalPlan> {
  let mut inputs = plan_node.inputs().iter()
      .map(|p| LogicalPlan::try_from(&**p))
      .collect::<OptResult<Vec<LogicalPlan>>>()?;

  match plan_node.operator() {
    Logical(LogicalProjection(projection)) => {
      let df_projection = DFProjection {
        expr: Vec::from(projection.expr()),
        input: Arc::new(inputs.remove(0)),
        schema: Arc::new(plan_node.logical_prop().unwrap().schema().clone()),
        alias: None,
      };

      Ok(LogicalPlan::Projection(df_projection))
    }
    Logical(LogicalLimit(limit)) => {
      let df_limit = DFLimit {
        n: limit.limit(),
        input: Arc::new(inputs.remove(0)),
      };

      Ok(LogicalPlan::Limit(df_limit))
    }
    Logical(LogicalJoin(join)) => {
      let df_join = DFJoin {
        left: Arc::new(inputs.remove(0)),
        right: Arc::new(inputs.remove(0)),
        on: expr_to_df_join_condition(join.expr())?,
        join_type: join.join_type(),
        join_constraint: JoinConstraint::On,
        schema: Arc::new(plan_node.logical_prop().unwrap().schema().clone()),
        null_equals_null: true,
      };

      Ok(LogicalPlan::Join(df_join))
    }
    Logical(LogicalScan(scan)) => {
      let schema = Arc::new(plan_node.logical_prop().unwrap().schema().clone());
      let source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(Arc::new
          ((&*schema).clone().into() )))));
      let df_scan = DFTableScan {
        table_name: scan.table_name().to_string(),
        source,
        projection: None,
        projected_schema: schema,
        filters: vec![],
        limit: scan.limit(),
      };

      Ok(LogicalPlan::TableScan(df_scan))
    },
    op => bail!("Can't convert plan to data fusion logical plan: {:?}", op)
  }
}

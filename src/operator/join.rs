use datafusion::prelude::JoinType;

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, PhysicalOperatorTrait};
use crate::optimizer::Optimizer;
use crate::properties::PhysicalPropertySet;
use crate::Expr;

/// Logical join operator.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Join {
    join_type: JoinType,
    expr: Expr,
}

impl Join {
    pub fn new(join_type: JoinType, expr: Expr) -> Self {
        Self { join_type, expr }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl PhysicalOperatorTrait for Join {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> OptResult<Vec<DerivePropResult>> {
        Ok(vec![DerivePropResult {
            output_prop: PhysicalPropertySet::default(),
            input_required_props: vec![
                PhysicalPropertySet::default(),
                PhysicalPropertySet::default(),
            ],
        }])
    }

    fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
        Ok(Cost::from(1.0))
    }
}

use datafusion::logical_plan::Expr;

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, PhysicalOperatorTrait};
use crate::optimizer::Optimizer;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Projection {
    expr: Expr,
}

impl Projection {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl PhysicalOperatorTrait for Projection {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> OptResult<Vec<DerivePropResult>> {
        todo!()
    }

    fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
        todo!()
    }
}

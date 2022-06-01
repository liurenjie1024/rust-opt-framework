use datafusion::logical_plan::Expr;

use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, PhysicalOperatorTrait};
use crate::optimizer::Optimizer;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Projection {
    expr: Vec<Expr>,
}

impl Projection {
    pub fn new<I: IntoIterator<Item = Expr>>(exprs: I) -> Self {
        Self { expr: exprs.into_iter().collect() }
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

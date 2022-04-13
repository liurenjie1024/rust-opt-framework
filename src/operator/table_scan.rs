use crate::cost::Cost;
use crate::error::OptResult;
use crate::operator::{DerivePropContext, DerivePropResult, PhysicalOperatorTrait};
use crate::optimizer::Optimizer;
use crate::properties::PhysicalPropertySet;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct TableScan {
    limit: Option<usize>,
    table_name: String,
}

impl TableScan {
    pub fn new<S: Into<String>>(table_name: S) -> Self {
        Self {
            limit: None,
            table_name: table_name.into(),
        }
    }

    pub fn with_limit<S: Into<String>>(table_name: S, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            table_name: table_name.into(),
        }
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl PhysicalOperatorTrait for TableScan {
    fn derive_properties<O: Optimizer>(
        &self,
        _context: DerivePropContext<O>,
    ) -> OptResult<Vec<DerivePropResult>> {
        Ok(vec![DerivePropResult {
            output_prop: PhysicalPropertySet::default(),
            input_required_props: vec![],
        }])
    }

    fn cost<O: Optimizer>(&self, _expr_handle: O::ExprHandle, _optimizer: &O) -> OptResult<Cost> {
        Ok(Cost::from(1.0))
    }
}

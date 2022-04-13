
use enum_as_inner::EnumAsInner;

use crate::operator::{Join, Limit, Projection, TableScan};

/// Logical relational operator.
#[derive(Clone, Debug, Hash, Eq, PartialEq, EnumAsInner)]
pub enum LogicalOperator {
    LogicalLimit(Limit),
    LogicalProjection(Projection),
    LogicalJoin(Join),
    LogicalScan(TableScan),
}

use std::rc::Rc;

use datafusion::logical_plan::DFSchema;

#[derive(Clone, PartialEq, Debug)]
pub struct LogicalProperty {
    schema: Rc<DFSchema>,
}

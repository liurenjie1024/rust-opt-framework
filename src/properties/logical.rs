use std::rc::Rc;

use datafusion::logical_plan::DFSchema;

#[derive(Clone, PartialEq, Debug)]
pub struct LogicalProperty {
    schema: Rc<DFSchema>,
}

impl LogicalProperty {
    pub fn new(schema: DFSchema) -> Self {
        Self {
            schema: Rc::new(schema)
        }
    }

    pub fn schema(&self) -> &DFSchema {
        &self.schema
    }
}

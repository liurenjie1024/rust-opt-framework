use std::collections::HashSet;
use std::mem::swap;
use std::rc::Rc;

use datafusion::prelude::{Expr, JoinType};

use crate::operator::LogicalOperator::{LogicalJoin, LogicalProjection, LogicalScan};
use crate::operator::Operator::{Logical, Physical};
use crate::operator::PhysicalOperator::{PhysicalHashJoin, PhysicalTableScan};
use crate::operator::{Join, Limit, LogicalOperator, Operator, Projection, TableScan};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::stat::Statistics;

pub type PlanNodeId = u32;

pub type PlanNodeRef = Rc<PlanNode>;

/// One node in a plan.
///
/// This is used in both input and output of an optimizer. Given that we may have many different
/// phases in query optimization, we use one data structure to represent a plan.
#[derive(Debug)]
pub struct PlanNode {
    id: PlanNodeId,
    operator: Operator,
    inputs: Vec<PlanNodeRef>,
    logical_prop: Option<LogicalProperty>,
    stat: Option<Statistics>,
    physical_props: Option<PhysicalPropertySet>,
}

/// The `eq` should ignore `id`.
impl PartialEq for PlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.inputs == other.inputs
            && self.logical_prop == other.logical_prop
            && self.stat == other.stat
            && self.physical_props == other.physical_props
    }
}

/// A query plan.
///
/// A query plan is a single root dag(directed acyclic graph). It can be used in many places, for
/// example, logical plan after validating an ast, a physical plan after completing optimizer.
#[derive(PartialEq, Debug)]
pub struct Plan {
    root: PlanNodeRef,
}

/// Breath first iterator of a single root dag plan.
struct BFSPlanNodeIter {
    visited: HashSet<PlanNodeId>,
    cur_level: Vec<PlanNodeRef>,
    next_level: Vec<PlanNodeRef>,
}

impl Iterator for BFSPlanNodeIter {
    type Item = PlanNodeRef;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_level.is_empty() {
            swap(&mut self.cur_level, &mut self.next_level);
        }

        if let Some(p) = self.cur_level.pop() {
            for input in &p.inputs {
                if !self.visited.contains(&input.id) {
                    self.next_level.push(input.clone());
                    self.visited.insert(input.id);
                }
            }

            Some(p)
        } else {
            None
        }
    }
}

impl Plan {
    pub fn new(root: PlanNodeRef) -> Self {
        Self { root }
    }

    pub fn root(&self) -> PlanNodeRef {
        self.root.clone()
    }

    pub fn bfs_iterator(&self) -> impl Iterator<Item = PlanNodeRef> {
        let mut visited = HashSet::new();
        visited.insert(self.root.id);

        BFSPlanNodeIter {
            cur_level: vec![self.root.clone()],
            next_level: vec![],
            visited,
        }
    }
}

impl PlanNode {
    pub fn new(id: PlanNodeId, operator: Operator, inputs: Vec<PlanNodeRef>) -> Self {
        Self {
            id,
            operator,
            inputs,
            logical_prop: None,
            stat: None,
            physical_props: None,
        }
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn id(&self) -> PlanNodeId {
        self.id
    }

    pub fn inputs(&self) -> &[PlanNodeRef] {
        &self.inputs
    }

    pub fn logical_prop(&self) -> Option<&LogicalProperty> {
        self.logical_prop.as_ref()
    }

    pub fn stat(&self) -> Option<&Statistics> {
        self.stat.as_ref()
    }

    pub fn physical_props(&self) -> Option<&PhysicalPropertySet> {
        self.physical_props.as_ref()
    }
}

pub struct PlanNodeBuilder {
    plan_node: PlanNode,
}

impl PlanNodeBuilder {
    pub fn new(id: PlanNodeId, operator: &Operator) -> Self {
        Self {
            plan_node: PlanNode {
                id,
                operator: operator.clone(),
                inputs: vec![],
                logical_prop: None,
                stat: None,
                physical_props: None,
            },
        }
    }

    pub fn add_inputs<I>(mut self, inputs: I) -> Self
    where
        I: IntoIterator<Item = PlanNodeRef>,
    {
        self.plan_node.inputs.extend(inputs);
        self
    }

    pub fn with_logical_prop(mut self, logical_prop: Option<LogicalProperty>) -> Self {
        self.plan_node.logical_prop = logical_prop;
        self
    }

    pub fn with_statistics(mut self, stat: Option<Statistics>) -> Self {
        self.plan_node.stat = stat;
        self
    }

    pub fn with_physical_props(mut self, physical_props: Option<PhysicalPropertySet>) -> Self {
        self.plan_node.physical_props = physical_props;
        self
    }

    pub fn build(self) -> PlanNode {
        self.plan_node
    }
}

pub struct LogicalPlanBuilder {
    root: Option<PlanNodeRef>,
    next_plan_node_id: PlanNodeId,
}

impl LogicalPlanBuilder {
    pub fn new() -> Self {
        Self {
            root: None,
            next_plan_node_id: 0,
        }
    }

    fn reset_root(&mut self, new_root: PlanNodeRef) -> &mut Self {
        self.root = Some(new_root);
        self.next_plan_node_id += 1;
        self
    }

    pub fn scan<S: Into<String>>(&mut self, limit: Option<usize>, table_name: S) -> &mut Self {
        let table_scan = match limit {
            Some(l) => TableScan::with_limit(table_name.into(), l),
            None => TableScan::new(table_name.into()),
        };
        let plan_node = Rc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalScan(table_scan)),
            vec![],
        ));

        self.reset_root(plan_node)
    }

    pub fn projection(&mut self, expr: Expr) -> &mut Self {
        let projection = Projection::new(expr);
        let plan_node = Rc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalProjection(projection)),
            vec![self.root.clone().unwrap()],
        ));

        self.reset_root(plan_node)
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        let limit = Limit::new(limit);
        let plan_node = Rc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalOperator::LogicalLimit(limit)),
            vec![self.root.clone().unwrap()],
        ));

        self.reset_root(plan_node)
    }

    pub fn join(&mut self, join_type: JoinType, condition: Expr, right: PlanNodeRef) -> &mut Self {
        let join = Join::new(join_type, condition);
        let plan_node = Rc::new(PlanNode::new(
            self.next_plan_node_id,
            Logical(LogicalJoin(join)),
            vec![self.root.clone().unwrap(), right],
        ));

        self.reset_root(plan_node)
    }

    /// Consume current plan, but not rest state, e.g. plan node id.
    ///
    /// This is useful for building multi child plan, e.g. join.
    pub fn build(&mut self) -> Plan {
        let ret = Plan {
            root: self.root.clone().unwrap(),
        };
        self.root = None;
        ret
    }
}

pub struct PhysicalPlanBuilder {
    root: PlanNodeRef,
    next_plan_node_id: PlanNodeId,
}

impl PhysicalPlanBuilder {
    fn reset_root(&mut self, new_root: PlanNodeRef) {
        self.root = new_root;
        self.next_plan_node_id += 1;
    }

    pub fn scan<S: Into<String>>(limit: Option<usize>, table_name: S) -> Self {
        let table_scan = match limit {
            Some(l) => TableScan::with_limit(table_name, l),
            None => TableScan::new(table_name.into()),
        };
        let plan_node = Rc::new(PlanNode::new(
            0,
            Physical(PhysicalTableScan(table_scan)),
            vec![],
        ));

        Self {
            root: plan_node,
            next_plan_node_id: 1,
        }
    }

    pub fn hash_join(mut self, join_type: JoinType, condition: Expr, right: PlanNodeRef) -> Self {
        let join = Join::new(join_type, condition);
        let plan_node = Rc::new(PlanNode::new(
            self.next_plan_node_id,
            Physical(PhysicalHashJoin(join)),
            vec![self.root.clone(), right],
        ));

        self.reset_root(plan_node);

        self
    }

    pub fn build(self) -> Plan {
        Plan { root: self.root }
    }
}

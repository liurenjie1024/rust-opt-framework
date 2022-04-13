use std::collections::HashMap;

use std::rc::Rc;

use petgraph::prelude::{NodeIndex, StableGraph};
use petgraph::visit::Bfs;
use petgraph::{Directed, Direction};

use crate::heuristic::{HepOptimizer, MatchOrder};
use crate::operator::Operator;
use crate::optimizer::{OptExpr, OptExprHandle, OptGroup, OptGroupHandle};
use crate::plan::{Plan, PlanNode, PlanNodeBuilder, PlanNodeId, PlanNodeRef};
use crate::properties::{LogicalProperty, PhysicalPropertySet};
use crate::rules::OptExprNode::{ExprHandleNode, GroupHandleNode, OperatorNode};
use crate::rules::OptExpression;
use crate::stat::Statistics;

type HepGraph = StableGraph<HepOptimizerNode, (), Directed, PlanNodeId>;
pub type HepNodeId = NodeIndex<PlanNodeId>;

pub struct HepOptimizerNode {
    id: HepNodeId,
    operator: Operator,
    logical_prop: Option<LogicalProperty>,
    stat: Option<Statistics>,
    physical_props: Option<PhysicalPropertySet>,
}

/// A plan should be a single root dag.
pub(super) struct PlanGraph {
    pub(super) graph: HepGraph,
    root: HepNodeId,
}

impl PlanGraph {
    pub(super) fn nodes_iter(
        &self,
        match_order: MatchOrder,
    ) -> Box<dyn Iterator<Item = HepNodeId>> {
        match match_order {
            MatchOrder::TopDown => Box::new(self.top_down_node_iters()),
            MatchOrder::BottomUp => Box::new(self.bottom_up_node_iters()),
        }
    }

    /// Replace relational expression with optimizer rule result.
    ///
    /// # Return
    ///
    /// The return value indicates whether graph changed.
    pub(super) fn replace_opt_expression(
        &mut self,
        opt_node: OptExpression<HepOptimizer>,
        origin_node_id: HepNodeId,
    ) -> bool {
        let new_hep_node_id = self.insert_opt_node(&opt_node);
        if new_hep_node_id != origin_node_id {
            // Redirect parents's child to new node
            let parent_node_ids: Vec<HepNodeId> = self
                .graph
                .neighbors_directed(origin_node_id, Direction::Incoming)
                .collect();
            for parent in parent_node_ids {
                self.graph.add_edge(parent, new_hep_node_id, ());
            }
            self.graph.remove_node(origin_node_id);

            if self.root == origin_node_id {
                self.root = new_hep_node_id;
            }

            true
        } else {
            false
        }
    }

    fn insert_opt_node(&mut self, opt_expr: &OptExpression<HepOptimizer>) -> HepNodeId {
        match opt_expr.node() {
            ExprHandleNode(expr_handle) => *expr_handle,
            GroupHandleNode(group_handle) => *group_handle,
            OperatorNode(operator) => {
                let input_hep_node_ids: Vec<HepNodeId> = opt_expr
                    .inputs()
                    .iter()
                    .map(|input_expr| self.insert_opt_node(&*input_expr))
                    .collect();

                let hep_node = HepOptimizerNode {
                    // Currently this id is fake.
                    id: HepNodeId::default(),
                    operator: operator.clone(),
                    logical_prop: None,
                    stat: None,
                    physical_props: None,
                };

                let new_node_id = self.graph.add_node(hep_node);
                // reset node id
                self.graph[new_node_id].id = new_node_id;
                for input_hep_node_id in input_hep_node_ids {
                    self.graph.add_edge(new_node_id, input_hep_node_id, ());
                }

                // TODO: Derive logical prop, stats here
                new_node_id
            }
        }
    }

    /// Return node ids in bottom up order.
    fn bottom_up_node_iters(&self) -> impl Iterator<Item = HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut bfs = Bfs::new(&self.graph, self.root);

        // Create plan node for each `HepOptimizerNode`
        while let Some(node_id) = bfs.next(&self.graph) {
            ids.push(node_id);
        }

        ids.into_iter().rev()
    }

    /// Return node ids in bottom up order.
    fn top_down_node_iters(&self) -> impl Iterator<Item = HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut bfs = Bfs::new(&self.graph, self.root);

        // Create plan node for each `HepOptimizerNode`
        while let Some(node_id) = bfs.next(&self.graph) {
            ids.push(node_id);
        }

        ids.into_iter()
    }

    pub(super) fn to_plan(&self) -> Plan {
        let next_plan_node_id = 1u32;
        let mut hep_node_id_to_plan_node = HashMap::<HepNodeId, PlanNodeRef>::new();
        // Traverse nodes in bottom up order, when visiting a node, its children all inserted
        // into map
        for node_id in self.bottom_up_node_iters() {
            let node = &self.graph[node_id];
            let inputs: Vec<PlanNodeRef> = self
                .graph
                .neighbors_directed(node_id, Direction::Outgoing)
                .map(|node_id| hep_node_id_to_plan_node.get(&node_id).unwrap().clone())
                .collect();

            let plan_node = PlanNodeBuilder::new(next_plan_node_id, &node.operator)
                .with_statistics(node.stat.clone())
                .with_logical_prop(node.logical_prop.clone())
                .with_physical_props(node.physical_props.clone())
                .add_inputs(inputs)
                .build();
            hep_node_id_to_plan_node.insert(node_id, Rc::new(plan_node));
        }

        hep_node_id_to_plan_node
            .get(&self.root)
            .map(|plan_node| Plan::new(plan_node.clone()))
            .unwrap()
    }
}

/// Converts from raw plan to plan graph.
impl From<Plan> for PlanGraph {
    fn from(plan: Plan) -> Self {
        let mut graph = HepGraph::default();
        let mut parents = HashMap::<PlanNodeId, Vec<PlanNodeId>>::new();
        let mut node_id_map = HashMap::<PlanNodeId, HepNodeId>::new();

        for plan_node_ref in plan.bfs_iterator() {
            for input in plan_node_ref.inputs() {
                parents
                    .entry(plan_node_ref.id())
                    .or_insert_with(Vec::new)
                    .push(input.id());
            }
            let plan_node = (&*plan_node_ref).into();
            let plan_node_id = graph.add_node(plan_node);
            graph[plan_node_id].id = plan_node_id;
            node_id_map.insert(plan_node_ref.id(), plan_node_id);
        }

        for (node_id, inputs) in parents {
            for input_id in inputs {
                graph.add_edge(
                    *node_id_map.get(&node_id).unwrap(),
                    *node_id_map.get(&input_id).unwrap(),
                    (),
                );
            }
        }

        Self {
            graph,
            root: *node_id_map.get(&(&*plan.root()).id()).unwrap(),
        }
    }
}

impl<'a> From<&'a PlanNode> for HepOptimizerNode {
    fn from(t: &'a PlanNode) -> Self {
        Self {
            id: HepNodeId::default(),
            operator: t.operator().clone(),
            logical_prop: t.logical_prop().cloned(),
            stat: t.stat().cloned(),
            physical_props: t.physical_props().cloned(),
        }
    }
}

impl OptGroup for HepOptimizerNode {}

impl OptExpr for HepOptimizerNode {
    type InputHandle = HepNodeId;
    type O = HepOptimizer;

    fn operator(&self) -> &Operator {
        &self.operator
    }

    fn inputs_len(&self, opt: &HepOptimizer) -> usize {
        opt.graph
            .graph
            .neighbors_directed(self.id, Direction::Outgoing)
            .count()
    }

    fn input_at(&self, idx: usize, opt: &HepOptimizer) -> HepNodeId {
        opt.graph
            .graph
            .neighbors_directed(self.id, Direction::Outgoing)
            .nth(idx)
            .unwrap()
    }
}

impl OptExprHandle for HepNodeId {
    type O = HepOptimizer;
}

impl OptGroupHandle for HepNodeId {
    type O = HepOptimizer;
}

//! Implementation of heuristic optimizer.
//!
//! Heuristic optimizer optimizes query plan by applying a batch of rewrite rules to query plan
//! until some condition is met, e.g. max number of iterations or reached fixed point. The
//! implementation is heavily inspired by [apache calcite](https://github.com/apache/calcite)'s
//! HepPlanner.

mod optimizer;
pub use optimizer::*;
mod graph;
pub use graph::*;
mod binding;

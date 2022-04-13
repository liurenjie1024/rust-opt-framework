//! ## Background
//!
//! The query optimizer accepts an unoptimized logical query plan, and outputs an optimized physical
//! plan ready to be executed. In general query optimization process comes in two flavors: rule
//! based and cost based.
//!
//! Rule based optimization is relative simple. We apply a collection of optimization rules to a
//! query plan repeatedly until some condition is met, for example, a fix point (plan no longer
//! changes) or number of times. The optimization rule is substitution rule, e.g. the optimizer
//! substitutes rule generated new plan for original plan, and in general the new plan should be
//! better than original plan. Rule based optimization is quite useful in applying some heuristic
//! optimization rules, for example, remove unnecessary field reference, column pruning, etc.
//!
//! Cost based optimization tries to find plan with lowest cost by searching plan space. [2]
//! proposed a top-down searching strategy to enumerate possible plans, and used dynamic
//! programming to reduce duplicated computation. This is also the cost based optimization
//! framework implemented in this crate. There are also other searching strategies, for example,
//! [1] used a bottom-up searching strategy to enumerate possible plans and also used dynamic
//! programming to reduce search time.
//!
//! ## Design
//!
//! ### Heuristic Optimizer
//!
//! Heuristic optimizer is a rule base optimizer. As mentioned above, it runs a batch of rules
//! iteratively, until reaching fix point or maximum number of iteration times. Heuristic
//! optimization is useful in several cases. For example, it can be used to preprocess logical
//! plan before sending to cost base optimizer. Also, in oltp or time series database which
//! serves highly concurrent point queries(plan is relative simple, and query result is small),
//! we may only use heuristic optimizer to reduce optimization time.
//!
//! ### Cascade Optimizer
//!
//!
//!
//! ## Reference
//!
//! 1. Selinger, P. Griffiths, et al. "Access path selection in a relational database management
//! system." Readings in Artificial Intelligence and Databases. Morgan Kaufmann, 1989. 511-522.
//! 2. Graefe, G., 1995. The cascades framework for query optimization. IEEE Data Eng. Bull., 18(3),
//! pp.19-29.

#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate core;

use datafusion::prelude::Expr;

pub mod cascades;
pub mod cost;
pub mod error;
pub mod heuristic;
pub mod operator;
pub mod optimizer;
pub mod plan;
pub mod properties;
pub mod rules;
pub mod stat;

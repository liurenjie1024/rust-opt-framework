//! Properties of relation operators.

mod distribution;

use std::fmt::Debug;
use std::hash::Hash;

pub use distribution::*;
mod order;
pub use order::*;
mod logical;
pub use logical::*;
mod physical;
pub use physical::*;

pub trait PhysicalProp: Debug + Hash {
    /// Tests whether satisfies self.
    fn satisfies(&self, other: &Self) -> bool;
}

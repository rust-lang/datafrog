//! A lightweight Datalog engine in Rust
//!
//! The intended design is that one has static `Relation` types that are sets
//! of tuples, and `Variable` types that represent monotonically increasing
//! sets of tuples.
//!
//! The types are mostly wrappers around `Vec<Tuple>` indicating sorted-ness,
//! and the intent is that this code can be dropped in the middle of an otherwise
//! normal Rust program, run to completion, and then the results extracted as
//! vectors again.

#![forbid(missing_docs)]

mod iteration;
mod join;
mod map;
mod merge;
mod relation;
mod test;
mod treefrog;
mod variable;

pub use crate::{
    iteration::Iteration,
    join::JoinInput,
    relation::Relation,
    treefrog::{
        extend_anti::ExtendAnti,
        extend_with::ExtendWith,
        filter_anti::FilterAnti,
        filter_with::FilterWith,
        filters::{passthrough, PrefixFilter, ValueFilter},
        Leaper, Leapers, RelationLeaper,
    },
    variable::Variable,
};

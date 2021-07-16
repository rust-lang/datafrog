//! Map functionality.

use super::{Relation, Variable};

pub(crate) fn map_into<Tuple1: Ord, Tuple2: Ord>(
    input: &Variable<Tuple1>,
    output: &Variable<Tuple2>,
    logic: impl FnMut(&Tuple1) -> Tuple2,
) {
    let results: Vec<Tuple2> = input.recent.borrow().iter().map(logic).collect();

    output.insert(Relation::from_vec(results));
}

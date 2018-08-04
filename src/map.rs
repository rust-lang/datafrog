//! Map functionality.

use super::{Variable, Relation};

pub fn map_into<T1: Ord, T2: Ord>(
    input: &Variable<T1>,
    output: &Variable<T2>,
    mut logic: impl FnMut(&T1)->T2) {

    let results: Vec<T2> = input.recent
        .borrow()
        .into_iter()
        .map(|tuple| logic(tuple))
        .collect();

    output.insert(Relation::from_vec(results));
}
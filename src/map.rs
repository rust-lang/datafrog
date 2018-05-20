//! Map functionality.

use super::{Variable, Relation};

pub fn map_into<T1: Ord, T2: Ord>(
    input: &Variable<T1>,
    output: &Variable<T2>,
    mut logic: impl FnMut(&T1)->T2) {

    let mut results = Vec::new();
    let recent = input.recent.borrow();
    for tuple in recent.iter() {
        results.push(logic(tuple));
    }

    output.insert(Relation::from_vec(results));
}
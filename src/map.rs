use super::Variable;

pub fn map_into<T1: Ord, T2: Ord, F: Fn(&T1)->T2>(
    input: &Variable<T1>,
    output: &Variable<T2>,
    logic: F) {

    let mut results = Vec::new();
    let recent = input.recent.borrow();
    for tuple in recent.iter() {
        results.push(logic(tuple));
    }

    output.insert(results.into());
}

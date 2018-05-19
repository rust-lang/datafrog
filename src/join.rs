//! Join functionality.

use super::{Variable, Relation};

pub fn join_into<Key: Ord, Val1: Ord, Val2: Ord, Result: Ord, F: Fn(&Key, &Val1, &Val2)->Result>(
    input1: &Variable<(Key, Val1)>,
    input2: &Variable<(Key, Val2)>,
    output: &Variable<Result>,
    logic: F) {

    let mut results = Vec::new();

    let recent1 = input1.recent.borrow();
    let recent2 = input2.recent.borrow();

    for batch2 in input2.stable.borrow().iter() {
        join_helper(&recent1, &batch2, |k,v1,v2| results.push(logic(k,v1,v2)));
    }

    for batch1 in input1.stable.borrow().iter() {
        join_helper(&batch1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)));
    }

    join_helper(&recent1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)));

    output.insert(Relation::from_vec(results));
}

/// Moves all recent tuples from `input1` that are not present in `input2` into `output`.
pub fn antijoin_into<Key: Ord, Val: Ord, Result: Ord, F: Fn(&Key, &Val)->Result>(
    input1: &Variable<(Key, Val)>,
    input2: &Relation<Key>,
    output: &Variable<Result>,
    logic: F) {

    let mut results = Vec::new();
    let mut tuples2 = &input2[..];

    for &(ref key, ref val) in input1.recent.borrow().iter() {
        tuples2 = gallop(tuples2, |k| k < key);
        if tuples2.first() != Some(key) {
            results.push(logic(key, val));
        }
    }

    output.insert(Relation::from_vec(results));
}

fn join_helper<K: Ord, V1, V2, F: FnMut(&K, &V1, &V2)>(mut slice1: &[(K,V1)], mut slice2: &[(K,V2)], mut result: F) {

    while !slice1.is_empty() && !slice2.is_empty() {

        use std::cmp::Ordering;

        // If the keys match produce tuples, else advance the smaller key until they might.
        match slice1[0].0.cmp(&slice2[0].0) {
            Ordering::Less => {
                slice1 = gallop(slice1, |x| x.0 < slice2[0].0);
            },
            Ordering::Equal => {

                // Determine the number of matching keys in each slice.
                let count1 = slice1.iter().take_while(|x| x.0 == slice1[0].0).count();
                let count2 = slice2.iter().take_while(|x| x.0 == slice2[0].0).count();

                // Produce results from the cross-product of matches.
                for index1 in 0 .. count1 {
                    for index2 in 0 .. count2 {
                        result(&slice1[0].0, &slice1[index1].1, &slice2[index2].1);
                    }
                }

                // Advance slices past this key.
                slice1 = &slice1[count1..];
                slice2 = &slice2[count2..];
            }
            Ordering::Greater => {
                slice2 = gallop(slice2, |x| x.0 < slice1[0].0);
            }
        }
    }
}

pub fn gallop<T, F: Fn(&T)->bool>(mut slice: &[T], cmp: F) -> &[T] {
    // if empty slice, or already >= element, return
    if slice.len() > 0 && cmp(&slice[0]) {
        let mut step = 1;
        while step < slice.len() && cmp(&slice[step]) {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && cmp(&slice[step]) {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    return slice;
}
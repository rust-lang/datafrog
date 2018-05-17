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

    for batch2 in input2.tuples.borrow().iter() {
        join_helper(&recent1, &batch2, |k,v1,v2| results.push(logic(k,v1,v2)));
    }

    for batch1 in input1.tuples.borrow().iter() {
        join_helper(&batch1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)));
    }

    join_helper(&recent1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)));

    output.insert(results.into());
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

    output.insert(results.into());
}

fn join_helper<K: Ord, V1, V2, F: FnMut(&K, &V1, &V2)>(mut slice1: &[(K,V1)], mut slice2: &[(K,V2)], mut result: F) {

    while !slice1.is_empty() && !slice2.is_empty() {

        if slice1[0].0 == slice2[0].0 {

            let mut key1_count = 1;
            while key1_count < slice1.len() && slice1[0].0 == slice1[key1_count].0 {
                key1_count += 1;
            }

            let mut key2_count = 1;
            while key2_count < slice2.len() && slice2[0].0 == slice2[key2_count].0 {
                key2_count += 1;
            }

            for index1 in 0 .. key1_count {
                for index2 in 0 .. key2_count {
                    result(&slice1[0].0, &slice1[index1].1, &slice2[index2].1);
                }
            }

            slice1 = &slice1[key1_count..];
            slice2 = &slice2[key2_count..];

        }
        else {

            if slice1[0].0 < slice2[0].0 {
                slice1 = gallop(slice1, |x| &x.0 < &slice2[0].0);
            }
            else {
                slice2 = gallop(slice2, |x| &x.0 < &slice1[0].0);
            }

        }
    }
}

#[inline(always)]
pub fn gallop<'a, T, F: Fn(&T)->bool>(mut slice: &'a [T], cmp: F) -> &'a [T] {
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
//! Join functionality.

use super::{Relation, Variable};
use std::cell::Ref;
use std::ops::Deref;

/// Implements `join`. Note that `input1` must be a variable, but
/// `input2` can be either a variable or a relation. This is necessary
/// because relations have no "recent" tuples, so the fn would be a
/// guaranteed no-op if both arguments were relations.  See also
/// `join_into_relation`.
pub(crate) fn join_into<'me, Key: Ord, Value1: Ord, Value2: Ord, Result: Ord>(
    input1: &Variable<(Key, Value1)>,
    input2: impl JoinInput<'me, (Key, Value2)>,
    output: &Variable<Result>,
    mut logic: impl FnMut(&Key, &Value1, &Value2) -> Result,
) {
    join_into_by_impl(
        input1,
        input2,
        output,
        |(key, _value)| key,
        |(key, _value)| key,
        |tuple1, tuple2| logic(&tuple1.0, &tuple1.1, &tuple2.1),
    )
}

pub(crate) fn join_into_by<
    'me,
    Key: Ord,
    Tuple1: Ord,
    Tuple2: Ord,
    Accessor1,
    Accessor2,
    Result: Ord,
>(
    input1: &Variable<Tuple1>,
    input2: impl JoinInput<'me, Tuple2>,
    output: &Variable<Result>,
    accessor1: Accessor1,
    accessor2: Accessor2,
    logic: impl FnMut(&Tuple1, &Tuple2) -> Result,
) where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    join_into_by_impl(input1, input2, output, accessor1, accessor2, logic)
}

#[inline(always)]
fn join_into_by_impl<'me, Key: Ord, Tuple1: Ord, Tuple2: Ord, Accessor1, Accessor2, Result: Ord>(
    input1: &Variable<Tuple1>,
    input2: impl JoinInput<'me, Tuple2>,
    output: &Variable<Result>,
    accessor1: Accessor1,
    accessor2: Accessor2,
    mut logic: impl FnMut(&Tuple1, &Tuple2) -> Result,
) where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    let mut results = Vec::new();

    let recent1 = input1.recent();
    let recent2 = input2.recent();

    {
        // scoped to let `closure` drop borrow of `results`.

        let mut closure = |tuple1: &Tuple1, tuple2: &Tuple2| results.push(logic(tuple1, tuple2));

        for batch2 in input2.stable().iter() {
            join_helper_by(&recent1, &batch2, &accessor1, &accessor2, &mut closure);
        }

        for batch1 in input1.stable().iter() {
            join_helper_by(&batch1, &recent2, &accessor1, &accessor2, &mut closure);
        }

        join_helper_by(&recent1, &recent2, &accessor1, &accessor2, &mut closure);
    }

    output.insert(Relation::from_vec(results));
}

/// Join, but for two relations.
pub(crate) fn join_into_relation<'me, Key: Ord, Value1: Ord, Value2: Ord, Result: Ord>(
    input1: &Relation<(Key, Value1)>,
    input2: &Relation<(Key, Value2)>,
    mut logic: impl FnMut(&Key, &Value1, &Value2) -> Result,
) -> Relation<Result> {
    join_into_relation_by(
        input1,
        input2,
        |(key, _value)| key,
        |(key, _value)| key,
        |tuple1, tuple2| logic(&tuple1.0, &tuple1.1, &tuple2.1),
    )
}

/// Join, but for two relations.
pub(crate) fn join_into_relation_by<
    'me,
    Key: Ord,
    Tuple1: Ord,
    Tuple2: Ord,
    Accessor1,
    Accessor2,
    Result: Ord,
>(
    input1: &Relation<Tuple1>,
    input2: &Relation<Tuple2>,
    accessor1: Accessor1,
    accessor2: Accessor2,
    mut logic: impl FnMut(&Tuple1, &Tuple2) -> Result,
) -> Relation<Result>
where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    let mut results = Vec::new();

    join_helper_by(
        &input1.elements,
        &input2.elements,
        &accessor1,
        &accessor2,
        |tuple1, tuple2| {
            results.push(logic(tuple1, tuple2));
        },
    );

    Relation::from_vec(results)
}

/// Moves all recent tuples from `input1` that are not present in `input2` into `output`.
pub(crate) fn antijoin<'me, Key: Ord, Value1: Ord, Result: Ord>(
    input1: impl JoinInput<'me, (Key, Value1)>,
    input2: &Relation<Key>,
    mut logic: impl FnMut(&Key, &Value1) -> Result,
) -> Relation<Result> {
    antijoin_by_impl(
        input1,
        input2,
        |(key, _value)| key,
        |key| key,
        |(key, value)| logic(key, value),
    )
}

pub(crate) fn antijoin_by<
    'me,
    Tuple1: Ord,
    Tuple2: Ord,
    Key: Ord,
    Accessor1,
    Accessor2,
    Result: Ord,
>(
    input1: impl JoinInput<'me, Tuple1>,
    input2: &Relation<Tuple2>,
    accessor1: Accessor1,
    accessor2: Accessor2,
    logic: impl FnMut(&Tuple1) -> Result,
) -> Relation<Result>
where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    antijoin_by_impl(input1, input2, accessor1, accessor2, logic)
}

/// Moves all recent tuples from `input1` that are not present in `input2` into `output`.
#[inline(always)]
pub(crate) fn antijoin_by_impl<
    'me,
    Tuple1: Ord,
    Tuple2: Ord,
    Key: Ord,
    Accessor1,
    Accessor2,
    Result: Ord,
>(
    input1: impl JoinInput<'me, Tuple1>,
    input2: &Relation<Tuple2>,
    accessor1: Accessor1,
    accessor2: Accessor2,
    mut logic: impl FnMut(&Tuple1) -> Result,
) -> Relation<Result>
where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    let mut tuples2 = &input2[..];

    let results = input1
        .recent()
        .iter()
        .filter(|tuple| {
            let key = accessor1(tuple);
            tuples2 = gallop(tuples2, |k| accessor2(k) < key);
            tuples2.first().map(|tuple2| accessor2(tuple2)) != Some(key)
        })
        .map(|tuple| logic(tuple))
        .collect::<Vec<_>>();

    Relation::from_vec(results)
}

#[allow(dead_code)]
fn join_helper<Key: Ord, Value1, Value2>(
    slice1: &[(Key, Value1)],
    slice2: &[(Key, Value2)],
    mut result: impl FnMut(&Key, &Value1, &Value2),
) {
    join_helper_by_impl(
        slice1,
        slice2,
        |(key, _value)| key,
        |(key, _value)| key,
        |tuple1, tuple2| result(&tuple1.0, &tuple1.1, &tuple2.1),
    )
}

fn join_helper_by<Key: Ord, Tuple1, Tuple2, Accessor1, Accessor2>(
    slice1: &[Tuple1],
    slice2: &[Tuple2],
    accessor1: Accessor1,
    accessor2: Accessor2,
    result: impl FnMut(&Tuple1, &Tuple2),
) where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    join_helper_by_impl(slice1, slice2, accessor1, accessor2, result)
}

#[inline(always)]
fn join_helper_by_impl<Key: Ord, Tuple1, Tuple2, Accessor1, Accessor2>(
    mut slice1: &[Tuple1],
    mut slice2: &[Tuple2],
    accessor1: Accessor1,
    accessor2: Accessor2,
    mut result: impl FnMut(&Tuple1, &Tuple2),
) where
    Accessor1: Fn(&Tuple1) -> &Key,
    Accessor2: Fn(&Tuple2) -> &Key,
{
    while !slice1.is_empty() && !slice2.is_empty() {
        use std::cmp::Ordering;

        let ordering = { accessor1(&slice1[0]).cmp(&accessor2(&slice2[0])) };

        // If the keys match produce tuples, else advance the smaller key until they might.
        match ordering {
            Ordering::Less => {
                slice1 = gallop(slice1, |x| accessor1(x) < accessor2(&slice2[0]));
            }
            Ordering::Equal => {
                // Determine the number of matching keys in each slice.
                let count1 = slice1
                    .iter()
                    .take_while(|x| accessor1(x) == accessor1(&slice1[0]))
                    .count();
                let count2 = slice2
                    .iter()
                    .take_while(|x| accessor2(x) == accessor2(&slice2[0]))
                    .count();

                // Produce results from the cross-product of matches.
                for index1 in 0..count1 {
                    for s2 in slice2[..count2].iter() {
                        result(&slice1[index1], s2);
                    }
                }

                // Advance slices past this key.
                slice1 = &slice1[count1..];
                slice2 = &slice2[count2..];
            }
            Ordering::Greater => {
                slice2 = gallop(slice2, |x| accessor2(x) < accessor1(&slice1[0]));
            }
        }
    }
}

pub(crate) fn gallop<Tuple>(mut slice: &[Tuple], mut cmp: impl FnMut(&Tuple) -> bool) -> &[Tuple] {
    // if empty slice, or already >= element, return
    if !slice.is_empty() && cmp(&slice[0]) {
        let mut step = 1;
        while step < slice.len() && cmp(&slice[step]) {
            slice = &slice[step..];
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if step < slice.len() && cmp(&slice[step]) {
                slice = &slice[step..];
            }
            step >>= 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    slice
}

/// An input that can be used with `from_join`; either a `Variable` or a `Relation`.
pub trait JoinInput<'me, Tuple: Ord>: Copy {
    /// If we are on iteration N of the loop, these are the tuples
    /// added on iteration N-1. (For a `Relation`, this is always an
    /// empty slice.)
    type RecentTuples: Deref<Target = [Tuple]>;

    /// If we are on iteration N of the loop, these are the tuples
    /// added on iteration N - 2 or before. (For a `Relation`, this is
    /// just `self`.)
    type StableTuples: Deref<Target = [Relation<Tuple>]>;

    /// Get the set of recent tuples.
    fn recent(self) -> Self::RecentTuples;

    /// Get the set of stable tuples.
    fn stable(self) -> Self::StableTuples;
}

impl<'me, Tuple: Ord> JoinInput<'me, Tuple> for &'me Variable<Tuple> {
    type RecentTuples = Ref<'me, [Tuple]>;
    type StableTuples = Ref<'me, [Relation<Tuple>]>;

    fn recent(self) -> Self::RecentTuples {
        Ref::map(self.recent.borrow(), |r| &r.elements[..])
    }

    fn stable(self) -> Self::StableTuples {
        Ref::map(self.stable.borrow(), |v| &v[..])
    }
}

impl<'me, Tuple: Ord> JoinInput<'me, Tuple> for &'me Relation<Tuple> {
    type RecentTuples = &'me [Tuple];
    type StableTuples = &'me [Relation<Tuple>];

    fn recent(self) -> Self::RecentTuples {
        &[]
    }

    fn stable(self) -> Self::StableTuples {
        std::slice::from_ref(self)
    }
}

use std::iter::FromIterator;

use crate::{
    join,
    merge,
    treefrog::{self, Leapers},
};

/// A static, ordered list of key-value pairs.
///
/// A relation represents a fixed set of key-value pairs. In many places in a
/// Datalog computation we want to be sure that certain relations are not able
/// to vary (for example, in antijoins).
#[derive(Clone)]
pub struct Relation<Tuple: Ord> {
    /// Sorted list of distinct tuples.
    pub elements: Vec<Tuple>,
}

impl<Tuple: Ord> Relation<Tuple> {
    /// Merges two relations into their union.
    pub fn merge(self, other: Self) -> Self {
        let elements = merge::merge_unique(self.elements, other.elements);
        Relation { elements }
    }

    /// Creates a `Relation` from the elements of the `iterator`.
    ///
    /// Same as the `from_iter` method from `std::iter::FromIterator` trait.
    pub fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item = Tuple>,
    {
        iterator.into_iter().collect()
    }

    /// Creates a `Relation` using the `leapjoin` logic;
    /// see [`Variable::from_leapjoin`]
    pub fn from_leapjoin<'leap, SourceTuple: Ord, Val: Ord + 'leap>(
        source: &Relation<SourceTuple>,
        leapers: impl Leapers<'leap, SourceTuple, Val>,
        logic: impl FnMut(&SourceTuple, &Val) -> Tuple,
    ) -> Self {
        treefrog::leapjoin(&source.elements, leapers, logic)
    }

    /// Creates a `Relation` by joining the values from `input1` and
    /// `input2` and then applying `logic`. Like
    /// [`Variable::from_join`] except for use where the inputs are
    /// not varying across iterations.
    pub fn from_join<Key: Ord, Value1: Ord, Value2: Ord>(
        input1: &Relation<(Key, Value1)>,
        input2: &Relation<(Key, Value2)>,
        logic: impl FnMut(&Key, &Value1, &Value2) -> Tuple,
    ) -> Self {
        join::join_into_relation(input1, input2, logic)
    }

    /// Creates a `Relation` by joining the values from `input1` and
    /// `input2`, through `accessor1` and `accessor2` respectively, and then applying `logic`. Like
    /// [`Variable::from_join_by`] except for use where the inputs are
    /// not varying across iterations.
    pub fn from_join_by<Key: Ord, Tuple1: Ord, Tuple2: Ord, Accessor1, Accessor2>(
        input1: &Relation<Tuple1>,
        input2: &Relation<Tuple2>,
        accessor1: Accessor1,
        accessor2: Accessor2,
        logic: impl FnMut(&Tuple1, &Tuple2) -> Tuple,
    ) -> Self
    where
        Accessor1: Fn(&Tuple1) -> &Key,
        Accessor2: Fn(&Tuple2) -> &Key,
    {
        join::join_into_relation_by(input1, input2, accessor1, accessor2, logic)
    }

    /// Creates a `Relation` by removing all values from `input1` that
    /// share a key with `input2`, and then transforming the resulting
    /// tuples with the `logic` closure. Like
    /// [`Variable::from_antijoin`] except for use where the inputs
    /// are not varying across iterations.
    pub fn from_antijoin<Key: Ord, Value1: Ord>(
        input1: &Relation<(Key, Value1)>,
        input2: &Relation<Key>,
        logic: impl FnMut(&Key, &Value1) -> Tuple,
    ) -> Self {
        join::antijoin(input1, input2, logic)
    }

    /// Creates a `Relation` by removing all values from `input1` that
    /// share a key with `input2`, through `accessor1` and `accessor2` respectively,
    /// and then transforming the resulting tuples with the `logic` closure.
    /// Like [`Variable::from_antijoin_by`] except for use where the inputs
    /// are not varying across iterations.
    pub fn from_antijoin_by<Key: Ord, Tuple1: Ord, Tuple2: Ord, Accessor1, Accessor2>(
        input1: &Relation<Tuple1>,
        input2: &Relation<Tuple2>,
        accessor1: Accessor1,
        accessor2: Accessor2,
        logic: impl FnMut(&Tuple1) -> Tuple,
    ) -> Self
    where
        Accessor1: Fn(&Tuple1) -> &Key,
        Accessor2: Fn(&Tuple2) -> &Key,
    {
        join::antijoin_by(input1, input2, accessor1, accessor2, logic)
    }

    /// Construct a new relation by mapping another one. Equivalent to
    /// creating an iterator but perhaps more convenient. Analogous to
    /// `Variable::from_map`.
    pub fn from_map<Tuple2: Ord>(
        input: &Relation<Tuple2>,
        logic: impl FnMut(&Tuple2) -> Tuple,
    ) -> Self {
        input.iter().map(logic).collect()
    }

    /// Creates a `Relation` from a vector of tuples.
    pub fn from_vec(mut elements: Vec<Tuple>) -> Self {
        elements.sort();
        elements.dedup();
        Relation { elements }
    }
}

impl<Tuple: Ord> From<Vec<Tuple>> for Relation<Tuple> {
    fn from(iterator: Vec<Tuple>) -> Self {
        Self::from_vec(iterator)
    }
}

impl<Tuple: Ord> FromIterator<Tuple> for Relation<Tuple> {
    fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item = Tuple>,
    {
        Relation::from_vec(iterator.into_iter().collect())
    }
}

impl<'tuple, Tuple: 'tuple + Copy + Ord> FromIterator<&'tuple Tuple> for Relation<Tuple> {
    fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item = &'tuple Tuple>,
    {
        Relation::from_vec(iterator.into_iter().cloned().collect())
    }
}

impl<Tuple: Ord> std::ops::Deref for Relation<Tuple> {
    type Target = [Tuple];
    fn deref(&self) -> &Self::Target {
        &self.elements[..]
    }
}

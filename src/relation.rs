use std::iter::FromIterator;

use crate::{
    join,
    merge,
    treefrog::{self, Leapers},
    Split,
};

/// A static, ordered list of key-value pairs.
///
/// A relation represents a fixed set of key-value pairs. In many places in a
/// Datalog computation we want to be sure that certain relations are not able
/// to vary (for example, in antijoins).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation<Tuple> {
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
    pub fn from_leapjoin<SourceTuple: Ord, Val: Ord>(
        source: &Relation<SourceTuple>,
        leapers: impl Leapers<SourceTuple, Val>,
        logic: impl FnMut(&SourceTuple, &Val) -> Tuple,
    ) -> Self {
        treefrog::leapjoin(&source.elements, leapers, logic)
    }

    /// Creates a `Relation` by joining the values from `input1` and
    /// `input2` and then applying `logic`. Like
    /// [`Variable::from_join`] except for use where the inputs are
    /// not varying across iterations.
    pub fn from_join<P, A, B>(
        input1: &Relation<A>,
        input2: &Relation<B>,
        logic: impl FnMut(P, A::Suffix, B::Suffix) -> Tuple,
    ) -> Self
    where
        P: Ord,
        A: Copy + Split<P>,
        B: Copy + Split<P>,
    {
        join::join_into_relation(input1, input2, logic)
    }

    /// An small wrapper around [`Relation::from_join`] that uses the first element of `A` and `B`
    /// as the shared prefix.
    ///
    /// This is useful because `Split` needs a tuple, and working with 1-tuples is a pain.
    /// It can also help with inference in cases where `logic` does not make use of the shared
    /// prefix.
    pub fn from_join_first<P, A, B>(
        input1: &Relation<A>,
        input2: &Relation<B>,
        mut logic: impl FnMut(P, A::Suffix, B::Suffix) -> Tuple,
    ) -> Self
    where
        P: Ord,
        A: Copy + Split<(P,)>,
        B: Copy + Split<(P,)>,
    {
        join::join_into_relation(input1, input2, |(p,), a, b| logic(p, a, b))
    }

    /// Creates a `Relation` by removing all values from `input1` that
    /// share a key with `input2`, and then transforming the resulting
    /// tuples with the `logic` closure. Like
    /// [`Variable::from_antijoin`] except for use where the inputs
    /// are not varying across iterations.
    pub fn from_antijoin<P, A>(
        input1: &Relation<A>,
        input2: &Relation<P>,
        logic: impl FnMut(A) -> Tuple,
    ) -> Self
    where
        P: Ord,
        A: Copy + Split<P>
    {
        join::antijoin(input1, input2, logic)
    }

    /// Construct a new relation by mapping another one. Equivalent to
    /// creating an iterator but perhaps more convenient. Analogous to
    /// `Variable::from_map`.
    pub fn from_map<T2: Ord>(input: &Relation<T2>, logic: impl FnMut(&T2) -> Tuple) -> Self {
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

impl<'tuple, Tuple: 'tuple + Clone + Ord> FromIterator<&'tuple Tuple> for Relation<Tuple> {
    fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item = &'tuple Tuple>,
    {
        Relation::from_vec(iterator.into_iter().cloned().collect())
    }
}

impl<Tuple> std::ops::Deref for Relation<Tuple> {
    type Target = [Tuple];
    fn deref(&self) -> &Self::Target {
        &self.elements[..]
    }
}

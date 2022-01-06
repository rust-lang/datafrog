//! Join functionality.

use super::Relation;

/// Performs treefrog leapjoin using a list of leapers.
pub(crate) fn leapjoin<Tuple: Ord, Val: Ord, Result: Ord>(
    source: &[Tuple],
    mut leapers: impl Leapers<Tuple, Val>,
    mut logic: impl FnMut(&Tuple, &Val) -> Result,
) -> Relation<Result> {
    let mut result = Vec::new(); // temp output storage.
    let mut values = Vec::new(); // temp value storage.

    for tuple in source {
        // Determine which leaper would propose the fewest values.
        let mut min_index = usize::max_value();
        let mut min_count = usize::max_value();
        leapers.for_each_count(tuple, |index, count| {
            if min_count > count {
                min_count = count;
                min_index = index;
            }
        });

        // We had best have at least one relation restricting values.
        assert!(min_count < usize::max_value());

        // If there are values to propose:
        if min_count > 0 {
            // Push the values that `min_index` "proposes" into `values`.
            leapers.propose(tuple, min_index, &mut values);

            // Give other leapers a chance to remove values from
            // anti-joins or filters.
            leapers.intersect(tuple, min_index, &mut values);

            // Push remaining items into result.
            for val in values.drain(..) {
                result.push(logic(tuple, &val));
            }
        }
    }

    Relation::from_vec(result)
}

/// Implemented for a tuple of leapers
pub trait Leapers<Tuple, Val> {
    /// Internal method:
    fn for_each_count(&mut self, tuple: &Tuple, op: impl FnMut(usize, usize));

    /// Internal method:
    fn propose(&mut self, tuple: &Tuple, min_index: usize, values: &mut Vec<Val>);

    /// Internal method:
    fn intersect(&mut self, tuple: &Tuple, min_index: usize, values: &mut Vec<Val>);
}

macro_rules! tuple_leapers {
    ($($Ty:ident)*) => {
        #[allow(unused_assignments, non_snake_case)]
        impl<Tuple, Val, $($Ty),*> Leapers<Tuple, Val> for ($($Ty,)*)
        where
            $($Ty: Leaper<Tuple, Val>,)*
        {
            fn for_each_count(&mut self, tuple: &Tuple, mut op: impl FnMut(usize, usize)) {
                let ($($Ty,)*) = self;
                let mut index = 0;
                $(
                    let count = $Ty.count(tuple);
                    op(index, count);
                    index += 1;
                )*
            }

            fn propose(&mut self, tuple: &Tuple, min_index: usize, values: &mut Vec<Val>) {
                let ($($Ty,)*) = self;
                let mut index = 0;
                $(
                    if min_index == index {
                        return $Ty.propose(tuple, values);
                    }
                    index += 1;
                )*
                    panic!("no match found for min_index={}", min_index);
            }

            fn intersect(&mut self, tuple: &Tuple, min_index: usize, values: &mut Vec<Val>) {
                let ($($Ty,)*) = self;
                let mut index = 0;
                $(
                    if min_index != index {
                        $Ty.intersect(tuple, values);
                    }
                    index += 1;
                )*
            }
        }
    }
}

tuple_leapers!(A B);
tuple_leapers!(A B C);
tuple_leapers!(A B C D);
tuple_leapers!(A B C D E);
tuple_leapers!(A B C D E F);
tuple_leapers!(A B C D E F G);

/// Methods to support treefrog leapjoin.
pub trait Leaper<Tuple, Val> {
    /// Estimates the number of proposed values.
    fn count(&mut self, prefix: &Tuple) -> usize;
    /// Populates `values` with proposed values.
    fn propose(&mut self, prefix: &Tuple, values: &mut Vec<Val>);
    /// Restricts `values` to proposed values.
    fn intersect(&mut self, prefix: &Tuple, values: &mut Vec<Val>);
}

pub(crate) mod filters {
    use super::Leaper;
    use super::Leapers;

    /// A treefrog leaper that tests each of the tuples from the main
    /// input (the "prefix"). Use like `PrefixFilter::from(|tuple|
    /// ...)`; if the closure returns true, then the tuple is
    /// retained, else it will be ignored. This leaper can be used in
    /// isolation in which case it just acts like a filter on the
    /// input (the "proposed value" will be `()` type).
    pub struct PrefixFilter<Tuple, Func: Fn(&Tuple) -> bool> {
        phantom: ::std::marker::PhantomData<Tuple>,
        predicate: Func,
    }

    impl<Tuple, Func> PrefixFilter<Tuple, Func>
    where
        Func: Fn(&Tuple) -> bool,
    {
        /// Creates a new filter based on the prefix
        pub fn from(predicate: Func) -> Self {
            PrefixFilter {
                phantom: ::std::marker::PhantomData,
                predicate,
            }
        }
    }

    impl<Tuple, Val, Func> Leaper<Tuple, Val> for PrefixFilter<Tuple, Func>
    where
        Func: Fn(&Tuple) -> bool,
    {
        /// Estimates the number of proposed values.
        fn count(&mut self, prefix: &Tuple) -> usize {
            if (self.predicate)(prefix) {
                usize::max_value()
            } else {
                0
            }
        }
        /// Populates `values` with proposed values.
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<Val>) {
            panic!("PrefixFilter::propose(): variable apparently unbound");
        }
        /// Restricts `values` to proposed values.
        fn intersect(&mut self, _prefix: &Tuple, _values: &mut Vec<Val>) {
            // We can only be here if we returned max_value() above.
        }
    }

    impl<Tuple, Func> Leapers<Tuple, ()> for PrefixFilter<Tuple, Func>
    where
        Func: Fn(&Tuple) -> bool,
    {
        fn for_each_count(&mut self, tuple: &Tuple, mut op: impl FnMut(usize, usize)) {
            if <Self as Leaper<Tuple, ()>>::count(self, tuple) == 0 {
                op(0, 0)
            } else {
                // we will "propose" the `()` value if the predicate applies
                op(0, 1)
            }
        }

        fn propose(&mut self, _: &Tuple, min_index: usize, values: &mut Vec<()>) {
            assert_eq!(min_index, 0);
            values.push(());
        }

        fn intersect(&mut self, _: &Tuple, min_index: usize, values: &mut Vec<()>) {
            assert_eq!(min_index, 0);
            assert_eq!(values.len(), 1);
        }
    }

    pub struct Passthrough<Tuple> {
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<Tuple> Passthrough<Tuple> {
        fn new() -> Self {
            Passthrough {
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<Tuple> Leaper<Tuple, ()> for Passthrough<Tuple> {
        /// Estimates the number of proposed values.
        fn count(&mut self, _prefix: &Tuple) -> usize {
            1
        }
        /// Populates `values` with proposed values.
        fn propose(&mut self, _prefix: &Tuple, values: &mut Vec<()>) {
            values.push(())
        }
        /// Restricts `values` to proposed values.
        fn intersect(&mut self, _prefix: &Tuple, _values: &mut Vec<()>) {
            // `Passthrough` never removes values (although if we're here it indicates that the user
            // didn't need a `Passthrough` in the first place)
        }
    }

    /// Returns a leaper that proposes a single copy of each tuple from the main input.
    ///
    /// Use this when you don't need any "extend" leapers in a join, only "filter"s. For example,
    /// in the following datalog rule, all terms in the second and third predicate are bound in the
    /// first one (the "main input" to our leapjoin).
    ///
    /// ```prolog
    /// error(loan, point) :-
    ///     origin_contains_loan_at(origin, loan, point), % main input
    ///     origin_live_at(origin, point),
    ///     loan_invalidated_at(loan, point).
    /// ```
    ///
    /// Without a passthrough leaper, neither the filter for `origin_live_at` nor the one for
    /// `loan_invalidated_at` would propose any tuples, and the leapjoin would panic at runtime.
    pub fn passthrough<Tuple>() -> Passthrough<Tuple> {
        Passthrough::new()
    }

    /// A treefrog leaper based on a predicate of prefix and value.
    /// Use like `ValueFilter::from(|tuple, value| ...)`. The closure
    /// should return true if `value` ought to be retained. The
    /// `value` will be a value proposed elsewhere by an `extend_with`
    /// leaper.
    ///
    /// This leaper cannot be used in isolation, it must be combined
    /// with other leapers.
    pub struct ValueFilter<Tuple, Val, Func: Fn(&Tuple, &Val) -> bool> {
        phantom: ::std::marker::PhantomData<(Tuple, Val)>,
        predicate: Func,
    }

    impl<Tuple, Val, Func> ValueFilter<Tuple, Val, Func>
    where
        Func: Fn(&Tuple, &Val) -> bool,
    {
        /// Creates a new filter based on the prefix
        pub fn from(predicate: Func) -> Self {
            ValueFilter {
                phantom: ::std::marker::PhantomData,
                predicate,
            }
        }
    }

    impl<Tuple, Val, Func> Leaper<Tuple, Val> for ValueFilter<Tuple, Val, Func>
    where
        Func: Fn(&Tuple, &Val) -> bool,
    {
        /// Estimates the number of proposed values.
        fn count(&mut self, _prefix: &Tuple) -> usize {
            usize::max_value()
        }
        /// Populates `values` with proposed values.
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<Val>) {
            panic!("PrefixFilter::propose(): variable apparently unbound");
        }
        /// Restricts `values` to proposed values.
        fn intersect(&mut self, prefix: &Tuple, values: &mut Vec<Val>) {
            values.retain(|val| (self.predicate)(prefix, val));
        }
    }
}

impl<T: Ord + Copy> Relation<T> {
    /// Extend with `<T as Split<P>>::Suffix` using the elements of the relation.
    ///
    /// This leaper proposes all tuples in `self` that have as a prefix the key extracted from the
    /// source tuple via `key_func`.
    ///
    /// This leaper is analagous to a join: it finds all sets of tuples in the source and in
    /// the underlying relation that have a shared prefix of type `P`, and for each shared prefix
    /// generates the cartesian product of the two sets.
    pub fn extend_with<P, F, S>(&self, key_func: F) -> extend_with::ExtendWith<'_, P, T, F>
        where F: Fn(&S) -> P // These bounds aren't necessary and could be deferred.
                             // They help with closure inference, however (see rust#41078).
    {
        extend_with::ExtendWith::from(self, key_func)
    }

    /// Extend with `<T as Split<P>>::Suffix` using the complement of the relation.
    ///
    /// This leaper *removes* proposed values when
    ///   * `key_func(src)` matches the prefix (`P`) of a tuple in this relation,
    ///   * *AND* the proposed value matches the suffix of that same tuple.
    ///
    /// It is used when a negative atom depends on a variable that is proposed by another leaper.
    /// For example:
    ///
    /// ```prolog
    /// var_init_at(V, Q) :-
    ///     var_init_at(V, P),   /* leapjoin source */
    ///     cfg_edge(P, Q),      /* extend_with     */
    ///     !var_moved_at(V, Q). /* extend_anti     */
    /// ```
    ///
    /// For each source tuple in `var_init_at`, `cfg_edge` will propose some number of CFG nodes
    /// (`Q`). The `!var_moved_at` atom should be expressed as `extend_anti(|(v, _p)| v)`. That is,
    /// it extracts `V` from the source tuple (the prefix), and eliminates proposed tuples with
    /// that prefix whose suffix is `Q`.
    ///
    /// **FIXME:** The fact that `P` determines both the prefix (in the source) *and* the suffix (the
    /// proposed value) is more restrictive than necessary. You could imagine a more complex program
    /// where the proposed value contains more information than we need for the negative atom.
    ///
    /// ```prolog
    /// x(A, B2, C2) :-
    ///   x(A, B1, C1),     /* leapjoin source     */
    ///   t(B1, C1, B2, C2) /* Proposes `(B2, C2)` */
    ///   !f(A, B2).        /* Doesn't use `C2`!   */
    /// ```
    ///
    /// That would require a separate `val_func` (in addition to `key_func`) to extract the
    /// relevant part of the proposed value.
    pub fn extend_anti<P, F, S>(&self, key_func: F) -> extend_anti::ExtendAnti<'_, P, T, F>
        where F: Fn(&S) -> P
    {
        extend_anti::ExtendAnti::from(self, key_func)
    }

    /// Extend with any value if tuple is present in relation.
    pub fn filter_with<F, S>(&self, key_func: F) -> filter_with::FilterWith<'_, T, F>
        where F: Fn(&S) -> T
    {
        filter_with::FilterWith::from(self, key_func)
    }

    /// Extend with any value if tuple is absent from relation.
    pub fn filter_anti<F, S>(&self, key_func: F) -> filter_anti::FilterAnti<'_, T, F>
        where F: Fn(&S) -> T
    {
        filter_anti::FilterAnti::from(self, key_func)
    }
}

pub(crate) mod extend_with {
    use super::{binary_search, Leaper, Leapers, Relation};
    use crate::join::gallop;
    use crate::Split;

    /// Wraps a `Relation<T>` as a leaper that proposes all values who have as a prefix the key
    /// extracted from the source tuple.
    pub struct ExtendWith<'a, P, T, F> {
        relation: &'a Relation<T>,
        start: usize,
        end: usize,
        old_key: Option<P>,
        key_func: F,
    }

    impl<'a, P, T, F> ExtendWith<'a, P, T, F> {
        /// Constructs an `ExtendWith` from a `Relation` and a key function.
        pub fn from(relation: &'a Relation<T>, key_func: F) -> Self {
            ExtendWith { relation, start: 0, end: 0, old_key: None, key_func }
        }
    }

    impl<P, T, S, F> Leaper<S, T::Suffix> for ExtendWith<'_, P, T, F>
    where
        T: Copy + Split<P>,
        P: Ord,
        T::Suffix: Ord,
        F: Fn(&S) -> P,
    {
        fn count(&mut self, src: &S) -> usize {
            let key = (self.key_func)(src);
            if self.old_key.as_ref() != Some(&key) {
                self.start = binary_search(&self.relation.elements, |x| &x.prefix() < &key);
                let slice1 = &self.relation[self.start..];
                let slice2 = gallop(slice1, |x| &x.prefix() <= &key);
                self.end = self.relation.len() - slice2.len();

                self.old_key = Some(key);
            }

            self.end - self.start
        }

        fn propose(&mut self, _src: &S, values: &mut Vec<T::Suffix>) {
            let slice = &self.relation[self.start..self.end];
            values.extend(slice.iter().map(|val| val.suffix()));
        }

        fn intersect(&mut self, _src: &S, values: &mut Vec<T::Suffix>) {
            let mut slice = &self.relation[self.start..self.end];
            values.retain(|v| {
                slice = gallop(slice, |kv| &kv.suffix() < v);
                slice.get(0).map(|kv| kv.suffix()).as_ref() == Some(v)
            });
        }
    }

    impl<P, T, S, F> Leapers<S, T::Suffix> for ExtendWith<'_, P, T, F>
    where
        T: Split<P>,
        Self: Leaper<S, T::Suffix>,
    {
        fn for_each_count(&mut self, tuple: &S, mut op: impl FnMut(usize, usize)) {
            op(0, self.count(tuple))
        }

        fn propose(&mut self, tuple: &S, min_index: usize, values: &mut Vec<T::Suffix>) {
            assert_eq!(min_index, 0);
            Leaper::propose(self, tuple, values);
        }

        fn intersect(&mut self, _: &S, min_index: usize, _: &mut Vec<T::Suffix>) {
            assert_eq!(min_index, 0);
        }
    }
}

pub(crate) mod extend_anti {
    use std::ops::Range;

    use super::{binary_search, Leaper, Relation};
    use crate::join::gallop;
    use crate::Split;

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct ExtendAnti<'a, P, T, F> {
        relation: &'a Relation<T>,
        key_func: F,
        old_key: Option<(P, Range<usize>)>,
    }

    impl<'a, P, T, F> ExtendAnti<'a, P, T, F> {
        /// Constructs a ExtendAnti from a relation and key and value function.
        pub fn from(relation: &'a Relation<T>, key_func: F) -> Self {
            ExtendAnti { relation, key_func, old_key: None }
        }
    }

    impl<P, T, S, F> Leaper<S, T::Suffix> for ExtendAnti<'_, P, T, F>
    where
        T: Copy + Split<P>,
        P: Ord,
        T::Suffix: Ord,
        F: Fn(&S) -> P,
    {
        fn count(&mut self, _prefix: &S) -> usize {
            usize::max_value()
        }
        fn propose(&mut self, _prefix: &S, _values: &mut Vec<T::Suffix>) {
            panic!("ExtendAnti::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, prefix: &S, values: &mut Vec<T::Suffix>) {
            let key = (self.key_func)(prefix);

            let range = match self.old_key.as_ref() {
                Some((old, range)) if old == &key => range.clone(),

                _ => {
                    let start = binary_search(&self.relation.elements, |x| &x.prefix() < &key);
                    let slice1 = &self.relation[start..];
                    let slice2 = gallop(slice1, |x| &x.prefix() <= &key);
                    let range = start..self.relation.len()-slice2.len();

                    self.old_key = Some((key, range.clone()));

                    range
                }
            };

            let mut slice = &self.relation[range];
            if !slice.is_empty() {
                values.retain(|v| {
                    slice = gallop(slice, |kv| &kv.suffix() < v);
                    slice.get(0).map(|kv| kv.suffix()).as_ref() != Some(v)
                });
            }
        }
    }
}

pub(crate) mod filter_with {

    use super::{Leaper, Leapers, Relation};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterWith<'a, T, F> {
        relation: &'a Relation<T>,
        key_func: F,
        old_key_val: Option<(T, bool)>,
    }

    impl<'a, T, F> FilterWith<'a, T, F> {
        /// Constructs a FilterWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<T>, key_func: F) -> Self {
            FilterWith { relation, key_func, old_key_val: None }
        }
    }

    impl<T, S, F, X> Leaper<S, X> for FilterWith<'_, T, F>
    where
        T: Ord,
        S: Ord,
        F: Fn(&S) -> T,
    {
        fn count(&mut self, prefix: &S) -> usize {
            let key_val = (self.key_func)(prefix);

            if let Some((ref old_key_val, is_present)) = self.old_key_val {
                if old_key_val == &key_val {
                    return if is_present { usize::MAX } else { 0 };
                }
            }

            let is_present = self.relation.binary_search(&key_val).is_ok();
            self.old_key_val = Some((key_val, is_present));
            if is_present { usize::MAX } else { 0 }
        }
        fn propose(&mut self, _prefix: &S, _values: &mut Vec<X>) {
            panic!("FilterWith::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, _prefix: &S, _values: &mut Vec<X>) {
            // Only here because we didn't return zero above, right?
        }
    }

    impl<T, S, F> Leapers<S, ()> for FilterWith<'_, T, F>
    where
        Self: Leaper<S, ()>,
    {
        fn for_each_count(&mut self, tuple: &S, mut op: impl FnMut(usize, usize)) {
            if <Self as Leaper<S, ()>>::count(self, tuple) == 0 {
                op(0, 0)
            } else {
                op(0, 1)
            }
        }

        fn propose(&mut self, _: &S, min_index: usize, values: &mut Vec<()>) {
            assert_eq!(min_index, 0);
            values.push(());
        }

        fn intersect(&mut self, _: &S, min_index: usize, values: &mut Vec<()>) {
            assert_eq!(min_index, 0);
            assert_eq!(values.len(), 1);
        }
    }
}

pub(crate) mod filter_anti {
    use super::{Leaper, Leapers, Relation};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterAnti<'a, T, F> {
        relation: &'a Relation<T>,
        key_func: F,
        old_key_val: Option<(T, bool)>,
    }

    impl<'a, T, F> FilterAnti<'a, T, F> {
        /// Constructs a FilterAnti from a relation and key and value function.
        pub fn from(relation: &'a Relation<T>, key_func: F) -> Self {
            FilterAnti {
                relation,
                key_func,
                old_key_val: None,
            }
        }
    }

    impl<T, S, F, X> Leaper<S, X> for FilterAnti<'_, T, F>
    where
        T: Ord,
        S: Ord,
        F: Fn(&S) -> T,
    {
        fn count(&mut self, prefix: &S) -> usize {
            let key_val = (self.key_func)(prefix);

            if let Some((ref old_key_val, is_present)) = self.old_key_val {
                if old_key_val == &key_val {
                    return if is_present { 0 } else { usize::MAX };
                }
            }

            let is_present = self.relation.binary_search(&key_val).is_ok();
            self.old_key_val = Some((key_val, is_present));
            if is_present { 0 } else { usize::MAX }
        }
        fn propose(&mut self, _prefix: &S, _values: &mut Vec<X>) {
            panic!("FilterAnti::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, _prefix: &S, _values: &mut Vec<X>) {
            // Only here because we didn't return zero above, right?
        }
    }

    impl<T, S, F> Leapers<S, ()> for FilterAnti<'_, T, F>
    where
        Self: Leaper<S, ()>,
    {
        fn for_each_count(&mut self, tuple: &S, mut op: impl FnMut(usize, usize)) {
            if <Self as Leaper<S, ()>>::count(self, tuple) == 0 {
                op(0, 0)
            } else {
                op(0, 1)
            }
        }

        fn propose(&mut self, _: &S, min_index: usize, values: &mut Vec<()>) {
            // We only get here if `tuple` is *not* a member of `self.relation`
            assert_eq!(min_index, 0);
            values.push(());
        }

        fn intersect(&mut self, _: &S, min_index: usize, values: &mut Vec<()>) {
            // We only get here if `tuple` is not a member of `self.relation`
            assert_eq!(min_index, 0);
            assert_eq!(values.len(), 1);
        }
    }
}

/// Returns the lowest index for which `cmp(&vec[i])` returns `true`, assuming `vec` is in sorted
/// order.
///
/// By accepting a vector instead of a slice, we can do a small optimization when computing the
/// midpoint.
fn binary_search<T>(vec: &Vec<T>, mut cmp: impl FnMut(&T) -> bool) -> usize {
    // The midpoint calculation we use below is only correct for vectors with less than `isize::MAX`
    // elements. This is always true for vectors of sized types but maybe not for ZSTs? Sorting
    // ZSTs doesn't make much sense, so just forbid it here.
    assert!(std::mem::size_of::<T>() > 0);

    // we maintain the invariant that `lo` many elements of `slice` satisfy `cmp`.
    // `hi` is maintained at the first element we know does not satisfy `cmp`.

    let mut hi = vec.len();
    let mut lo = 0;
    while lo < hi {
        // Unlike in the general case, this expression cannot overflow because `Vec` is limited to
        // `isize::MAX` capacity and we disallow ZSTs above. If we needed to support slices or
        // vectors of ZSTs, which don't have an upper bound on their size AFAIK, we would need to
        // use a slightly less efficient version that cannot overflow: `lo + (hi - lo) / 2`.
        let mid = (hi + lo) / 2;

        // LLVM seems to be unable to prove that `mid` is always less than `vec.len()`, so use
        // `get_unchecked` to avoid a bounds check since this code is hot.
        let el: &T = unsafe { vec.get_unchecked(mid) };
        if cmp(el) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

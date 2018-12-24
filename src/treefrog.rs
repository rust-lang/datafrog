//! Join functionality.

use super::{Relation, Variable};

/// Performs treefrog leapjoin using a list of leapers.
pub fn leapjoin_into<'a, Tuple: Ord, Val: Ord + 'a, Result: Ord>(
    source: &Variable<Tuple>,
    leapers: &mut [&mut dyn Leaper<'a, Tuple, Val>],
    output: &Variable<Result>,
    mut logic: impl FnMut(&Tuple, &Val) -> Result,
) {
    let mut result = Vec::new(); // temp output storage.
    let mut values = Vec::new(); // temp value storage.

    for tuple in source.recent.borrow().iter() {
        // Determine which leaper would propose the fewest values.
        let mut min_index = usize::max_value();
        let mut min_count = usize::max_value();
        for index in 0..leapers.len() {
            let count = leapers[index].count(tuple);
            if min_count > count {
                min_count = count;
                min_index = index;
            }
        }

        // We had best have at least one relation restricting values.
        assert!(min_count < usize::max_value());

        // If there are values to propose ..
        if min_count > 0 {
            // Propose them, ..
            leapers[min_index].propose(tuple, &mut values);

            // Intersect them, ..
            for index in 0..leapers.len() {
                if index != min_index {
                    leapers[index].intersect(tuple, &mut values);
                }
            }

            // Respond to each of them.
            for val in values.drain(..) {
                result.push(logic(tuple, val));
            }
        }
    }

    output.insert(result.into());
}

/// Methods to support treefrog leapjoin.
pub trait Leaper<'a, Tuple, Val> {
    /// Estimates the number of proposed values.
    fn count(&mut self, prefix: &Tuple) -> usize;
    /// Populates `values` with proposed values.
    fn propose(&mut self, prefix: &Tuple, values: &mut Vec<&'a Val>);
    /// Restricts `values` to proposed values.
    fn intersect(&mut self, prefix: &Tuple, values: &mut Vec<&'a Val>);
}

pub mod filters {

    use super::Leaper;

    /// A treefrog leaper based on a per-prefix predicate.
    pub struct PrefixFilter<Tuple, Func: Fn(&Tuple) -> bool> {
        phantom: ::std::marker::PhantomData<Tuple>,
        predicate: Func,
    }

    impl<'a, Tuple, Func> PrefixFilter<Tuple, Func>
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

    impl<'a, Tuple, Val, Func> Leaper<'a, Tuple, Val> for PrefixFilter<Tuple, Func>
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
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val>) {
            panic!("PrefixFilter::propose(): variable apparently unbound");
        }
        /// Restricts `values` to proposed values.
        fn intersect(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val>) {
            // We can only be here if we returned max_value() above.
        }
    }

    /// A treefrog leaper based on a predicate of prefix and value.
    pub struct ValueFilter<Tuple, Val, Func: Fn(&Tuple, &Val) -> bool> {
        phantom: ::std::marker::PhantomData<(Tuple, Val)>,
        predicate: Func,
    }

    impl<'a, Tuple, Val, Func> ValueFilter<Tuple, Val, Func>
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

    impl<'a, Tuple, Val, Func> Leaper<'a, Tuple, Val> for ValueFilter<Tuple, Val, Func>
    where
        Func: Fn(&Tuple, &Val) -> bool,
    {
        /// Estimates the number of proposed values.
        fn count(&mut self, _prefix: &Tuple) -> usize {
            usize::max_value()
        }
        /// Populates `values` with proposed values.
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val>) {
            panic!("PrefixFilter::propose(): variable apparently unbound");
        }
        /// Restricts `values` to proposed values.
        fn intersect(&mut self, prefix: &Tuple, values: &mut Vec<&'a Val>) {
            values.retain(|val| (self.predicate)(prefix, val));
        }
    }

}

/// Extension method for relations.
pub trait RelationLeaper<Key: Ord, Val: Ord> {
    /// Extend with `Val` using the elements of the relation.
    fn extend_with<'a, Tuple: Ord, Func: Fn(&Tuple) -> Key>(
        &'a self,
        key_func: Func,
    ) -> extend_with::ExtendWith<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a;
    /// Extend with `Val` using the complement of the relation.
    fn extend_anti<'a, Tuple: Ord, Func: Fn(&Tuple) -> Key>(
        &'a self,
        key_func: Func,
    ) -> extend_anti::ExtendAnti<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a;
    /// Extend with any value if tuple is present in relation.
    fn filter_with<'a, Tuple: Ord, Func: Fn(&Tuple) -> (Key, Val)>(
        &'a self,
        key_func: Func,
    ) -> filter_with::FilterWith<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a;
    /// Extend with any value if tuple is absent from relation.
    fn filter_anti<'a, Tuple: Ord, Func: Fn(&Tuple) -> (Key, Val)>(
        &'a self,
        key_func: Func,
    ) -> filter_anti::FilterAnti<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a;
}

impl<Key: Ord, Val: Ord> RelationLeaper<Key, Val> for Relation<(Key, Val)> {
    fn extend_with<'a, Tuple: Ord, Func: Fn(&Tuple) -> Key>(
        &'a self,
        key_func: Func,
    ) -> extend_with::ExtendWith<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a,
    {
        extend_with::ExtendWith::from(self, key_func)
    }
    fn extend_anti<'a, Tuple: Ord, Func: Fn(&Tuple) -> Key>(
        &'a self,
        key_func: Func,
    ) -> extend_anti::ExtendAnti<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a,
    {
        extend_anti::ExtendAnti::from(self, key_func)
    }
    fn filter_with<'a, Tuple: Ord, Func: Fn(&Tuple) -> (Key, Val)>(
        &'a self,
        key_func: Func,
    ) -> filter_with::FilterWith<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a,
    {
        filter_with::FilterWith::from(self, key_func)
    }
    fn filter_anti<'a, Tuple: Ord, Func: Fn(&Tuple) -> (Key, Val)>(
        &'a self,
        key_func: Func,
    ) -> filter_anti::FilterAnti<'a, Key, Val, Tuple, Func>
    where
        Key: 'a,
        Val: 'a,
    {
        filter_anti::FilterAnti::from(self, key_func)
    }
}

pub(crate) mod extend_with {
    use super::{binary_search, Leaper, Relation};
    use crate::join::gallop;

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct ExtendWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        relation: &'a Relation<(Key, Val)>,
        start: usize,
        end: usize,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key, Val, Tuple, Func> ExtendWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        /// Constructs a ExtendWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            ExtendWith {
                relation,
                start: 0,
                end: 0,
                key_func,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key, Val, Tuple, Func> Leaper<'a, Tuple, Val> for ExtendWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        fn count(&mut self, prefix: &Tuple) -> usize {
            let key = (self.key_func)(prefix);
            self.start = binary_search(&self.relation[..], |x| &x.0 < &key);
            let slice1 = &self.relation[self.start..];
            let slice2 = gallop(slice1, |x| &x.0 <= &key);
            self.end = self.relation.len() - slice2.len();
            slice1.len() - slice2.len()
        }
        fn propose(&mut self, _prefix: &Tuple, values: &mut Vec<&'a Val>) {
            let slice = &self.relation[self.start..self.end];
            values.extend(slice.iter().map(|&(_, ref val)| val));
        }
        fn intersect(&mut self, _prefix: &Tuple, values: &mut Vec<&'a Val>) {
            let mut slice = &self.relation[self.start..self.end];
            values.retain(|v| {
                slice = gallop(slice, |kv| &kv.1 < v);
                slice.get(0).map(|kv| &kv.1) == Some(v)
            });
        }
    }
}

pub(crate) mod extend_anti {
    use super::{binary_search, Leaper, Relation};
    use crate::join::gallop;

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct ExtendAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        relation: &'a Relation<(Key, Val)>,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key, Val, Tuple, Func> ExtendAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        /// Constructs a ExtendAnti from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            ExtendAnti {
                relation,
                key_func,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key: Ord, Val: Ord + 'a, Tuple: Ord, Func: Fn(&Tuple) -> Key> Leaper<'a, Tuple, Val>
        for ExtendAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> Key,
    {
        fn count(&mut self, _prefix: &Tuple) -> usize {
            usize::max_value()
        }
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val>) {
            panic!("ExtendAnti::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, prefix: &Tuple, values: &mut Vec<&'a Val>) {
            let key = (self.key_func)(prefix);
            let start = binary_search(&self.relation[..], |x| &x.0 < &key);
            let slice1 = &self.relation[start..];
            let slice2 = gallop(slice1, |x| &x.0 <= &key);
            let mut slice = &slice1[..(slice1.len() - slice2.len())];
            if !slice.is_empty() {
                values.retain(|v| {
                    slice = gallop(slice, |kv| &kv.1 < v);
                    slice.get(0).map(|kv| &kv.1) != Some(v)
                });
            }
        }
    }
}

pub(crate) mod filter_with {

    use super::{Leaper, Relation};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        relation: &'a Relation<(Key, Val)>,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key, Val, Tuple, Func> FilterWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        /// Constructs a FilterWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            FilterWith {
                relation,
                key_func,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key, Val, Val2, Tuple, Func> Leaper<'a, Tuple, Val2>
        for FilterWith<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        fn count(&mut self, prefix: &Tuple) -> usize {
            let key_val = (self.key_func)(prefix);
            if self.relation.binary_search(&key_val).is_ok() {
                usize::max_value()
            } else {
                0
            }
        }
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val2>) {
            panic!("FilterWith::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val2>) {
            // Only here because we didn't return zero above, right?
        }
    }
}

pub(crate) mod filter_anti {

    use super::{Leaper, Relation};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        relation: &'a Relation<(Key, Val)>,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key, Val, Tuple, Func> FilterAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        /// Constructs a FilterAnti from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            FilterAnti {
                relation,
                key_func,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key: Ord, Val: Ord + 'a, Val2, Tuple: Ord, Func: Fn(&Tuple) -> (Key, Val)>
        Leaper<'a, Tuple, Val2> for FilterAnti<'a, Key, Val, Tuple, Func>
    where
        Key: Ord + 'a,
        Val: Ord + 'a,
        Tuple: Ord,
        Func: Fn(&Tuple) -> (Key, Val),
    {
        fn count(&mut self, prefix: &Tuple) -> usize {
            let key_val = (self.key_func)(prefix);
            if self.relation.binary_search(&key_val).is_ok() {
                0
            } else {
                usize::max_value()
            }
        }
        fn propose(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val2>) {
            panic!("FilterAnti::propose(): variable apparently unbound.");
        }
        fn intersect(&mut self, _prefix: &Tuple, _values: &mut Vec<&'a Val2>) {
            // Only here because we didn't return zero above, right?
        }
    }
}

fn binary_search<T>(slice: &[T], mut cmp: impl FnMut(&T) -> bool) -> usize {
    // we maintain the invariant that `lo` many elements of `slice` satisfy `cmp`.
    // `hi` is maintained at the first element we know does not satisfy `cmp`.

    let mut hi = slice.len();
    let mut lo = 0;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if cmp(&slice[mid]) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

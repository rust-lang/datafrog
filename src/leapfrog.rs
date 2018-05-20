//! Join functionality.

use super::{Variable, Relation};

/// Performs leapfrog join using a list of leapers.
pub fn leapfrog_into<'a, Tuple: Ord, Val: Ord+'a, Result: Ord>(
    source: &Variable<Tuple>,
    leapers: &mut [&mut LeapFrog<'a, Tuple, Val>],
    output: &Variable<Result>,
    mut logic: impl FnMut(&Tuple, &Val)->Result) {

    let mut result = Vec::new();

    for tuple in source.recent.borrow().iter() {

        for leaper in leapers.iter_mut() {
            leaper.seek_prefix(tuple);
        }

        while !leapers.iter().any(|l| l.is_empty()) {
            // for leaper in leapers.iter() { println!("{:?}, {:?}", leaper.peek_val().is_some(), leaper.is_empty()); }
            let val = leapers.iter_mut().flat_map(|l| l.peek_val()).max().expect("No maximum found");
            let mut present = true;
            for leaper in leapers.iter_mut() {
                if !leaper.seek_val(&val) { present = false; }
            }
            if present {
                result.push(logic(&tuple, &val));
            }
        }

    }

    output.insert(result.into());
}

/// Methods to support leapfrog navigation.
pub trait LeapFrog<'a,Tuple,Val> {
    /// Sets the cursor for a specific key.
    fn seek_prefix(&mut self, prefix: &Tuple);
    /// Seeks a specific key and value pair.
    ///
    /// This method positions the cursor just after `val`,
    /// under the assumption that we will not seek for the
    /// same value immediately afterwards.
    fn seek_val(&mut self, val: &Val) -> bool;
    /// Proposes a next value.
    ///
    /// It is not mandatory that this method return a value,
    /// but we will only observe extensions in the union of
    /// peeked values. This allows antijoin implementations
    /// to implement this interface, as long as at least one
    /// normal join participates.
    fn peek_val(&self) -> Option<&'a Val>;
    /// Indicates no further values available.
    fn is_empty(&self) -> bool;
}

/// Extension method for relations.
pub trait Leaper<Key: Ord, Val: Ord> {
    /// Extend with `Val` using the elements of the relation.
    fn extend_with<'a, Tuple: Ord, Func: Fn(&Tuple)->Key>(&'a self, key_func: Func) -> extend_with::ExtendWith<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a;
    /// Extend with `Val` using the complement of the relation.
    fn extend_anti<'a, Tuple: Ord, Func: Fn(&Tuple)->Key>(&'a self, key_func: Func) -> extend_anti::ExtendAnti<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a;
    /// Extend with any value if tuple is present in relation.
    fn filter_with<'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)>(&'a self, key_func: Func) -> filter_with::FilterWith<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a;
    /// Extend with any value if tuple is absent from relation.
    fn filter_anti<'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)>(&'a self, key_func: Func) -> filter_anti::FilterAnti<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a;
}

impl<Key: Ord, Val: Ord> Leaper<Key, Val> for Relation<(Key, Val)> {
    fn extend_with<'a, Tuple: Ord, Func: Fn(&Tuple)->Key>(&'a self, key_func: Func) -> extend_with::ExtendWith<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a {
        extend_with::ExtendWith::from(self, key_func)
    }
    fn extend_anti<'a, Tuple: Ord, Func: Fn(&Tuple)->Key>(&'a self, key_func: Func) -> extend_anti::ExtendAnti<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a {
        extend_anti::ExtendAnti::from(self, key_func)
    }
    fn filter_with<'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)>(&'a self, key_func: Func) -> filter_with::FilterWith<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a {
        filter_with::FilterWith::from(self, key_func)
    }
    fn filter_anti<'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)>(&'a self, key_func: Func) -> filter_anti::FilterAnti<'a, Key, Val, Tuple, Func> where Key: 'a, Val: 'a {
        filter_anti::FilterAnti::from(self, key_func)
    }
}

mod extend_with {

    use super::{Relation, LeapFrog, gallop};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct ExtendWith<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> {
        relation: &'a Relation<(Key, Val)>,
        start: usize,
        end: usize,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> ExtendWith<'a, Key, Val, Tuple, Func> {
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

    impl<'a, Key: Ord, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> LeapFrog<'a, Tuple,Val> for ExtendWith<'a, Key, Val, Tuple, Func> {
        fn seek_prefix(&mut self, tuple: &Tuple) {
            let key = (self.key_func)(tuple);
            let slice1 = gallop(&self.relation[..], |x| &x.0 < &key);
            let slice2 = gallop(slice1, |x| &x.0 <= &key);
            self.start = self.relation.len() - slice1.len();
            self.end = self.relation.len() - slice2.len();
            assert!(self.start <= self.end);
        }
        fn seek_val(&mut self, val: &Val) -> bool {
            assert!(self.start <= self.end);
            // Attempt to position the cursor at (key, val).
            if !self.is_empty() {
                let slice = gallop(&self.relation[self.start .. self.end], |x| &x.1 < val);
                self.start = self.end - slice.len();
            }
            // If the cursor is positioned at something that yields `val`, then success.
            if self.peek_val() == Some(val) {
                self.start += 1;
                true
            }
            else {
                false
            }
        }
        fn peek_val(&self) -> Option<&'a Val> {
            assert!(self.start <= self.end);
            if self.start < self.end {
                Some(&self.relation[self.start].1)
            }
            else {
                None
            }
        }
        fn is_empty(&self) -> bool {
            assert!(self.start <= self.end);
            self.start == self.end
        }
    }
}

mod extend_anti {

    use super::{Relation, LeapFrog, gallop};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct ExtendAnti<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> {
        relation: &'a Relation<(Key, Val)>,
        start: usize,
        end: usize,
        key_func: Func,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> ExtendAnti<'a, Key, Val, Tuple, Func> {
        /// Constructs a ExtendWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            ExtendAnti {
                relation,
                start: 0,
                end: 0,
                key_func,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key: Ord, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->Key> LeapFrog<'a, Tuple,Val> for ExtendAnti<'a, Key, Val, Tuple, Func> {
        fn seek_prefix(&mut self, tuple: &Tuple) {
            let key = (self.key_func)(tuple);
            let slice1 = gallop(&self.relation[..], |x| &x.0 < &key);
            let slice2 = gallop(slice1, |x| &x.0 <= &key);
            self.start = self.relation.len() - slice1.len();
            self.end = self.relation.len() - slice2.len();
            assert!(self.start <= self.end);
        }
        fn seek_val(&mut self, val: &Val) -> bool {
            assert!(self.start <= self.end);
            // Attempt to position the cursor at (key, val).
            if !self.is_empty() {
                let slice = gallop(&self.relation[self.start .. self.end], |x| &x.1 < val);
                self.start = self.end - slice.len();
            }
            // If the cursor is positioned at something that yields `val`, then success.
            if self.start < self.end && &self.relation[self.start].1 == val {
                self.start += 1;
                false
            }
            else {
                true
            }
        }
        fn peek_val(&self) -> Option<&'a Val> {
            None
        }
        fn is_empty(&self) -> bool {
            false
        }
    }
}

mod filter_with {

    use super::{Relation, LeapFrog};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterWith<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> {
        relation: &'a Relation<(Key, Val)>,
        key_func: Func,
        found: bool,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> FilterWith<'a, Key, Val, Tuple, Func> {
        /// Constructs a ExtendWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            FilterWith {
                relation,
                key_func,
                found: false,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key: Ord, Val: Ord+'a, Val2, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> LeapFrog<'a,Tuple,Val2> for FilterWith<'a, Key, Val, Tuple, Func> {
        fn seek_prefix(&mut self, tuple: &Tuple) {
            let key_val = (self.key_func)(tuple);
            self.found = self.relation.binary_search(&key_val).is_ok();
        }
        fn seek_val(&mut self, _val: &Val2) -> bool {
            self.found
        }
        fn peek_val(&self) -> Option<&'a Val2> {
            None
        }
        fn is_empty(&self) -> bool {
            !self.found
        }
    }
}

mod filter_anti {

    use super::{Relation, LeapFrog};

    /// Wraps a Relation<Tuple> as a leaper.
    pub struct FilterAnti<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> {
        relation: &'a Relation<(Key, Val)>,
        key_func: Func,
        found: bool,
        phantom: ::std::marker::PhantomData<Tuple>,
    }

    impl<'a, Key: Ord+'a, Val: Ord+'a, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> FilterAnti<'a, Key, Val, Tuple, Func> {
        /// Constructs a ExtendWith from a relation and key and value function.
        pub fn from(relation: &'a Relation<(Key, Val)>, key_func: Func) -> Self {
            FilterAnti {
                relation,
                key_func,
                found: false,
                phantom: ::std::marker::PhantomData,
            }
        }
    }

    impl<'a, Key: Ord, Val: Ord+'a, Val2, Tuple: Ord, Func: Fn(&Tuple)->(Key,Val)> LeapFrog<'a,Tuple,Val2> for FilterAnti<'a, Key, Val, Tuple, Func> {
        fn seek_prefix(&mut self, tuple: &Tuple) {
            let key_val = (self.key_func)(tuple);
            self.found = self.relation.binary_search(&key_val).is_ok();
        }
        fn seek_val(&mut self, _val: &Val2) -> bool {
            !self.found
        }
        fn peek_val(&self) -> Option<&'a Val2> {
            None
        }
        fn is_empty(&self) -> bool {
            self.found
        }
    }
}


fn gallop<T>(mut slice: &[T], mut cmp: impl FnMut(&T)->bool) -> &[T] {
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
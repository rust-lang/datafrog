use std::cell::RefCell;
use std::io::Write;
use std::iter::FromIterator;
use std::rc::Rc;

use crate::{
    join::{self, JoinInput},
    map,
    relation::Relation,
    treefrog::{self, Leapers},
};

/// A type that can report on whether it has changed.
pub(crate) trait VariableTrait {
    /// Reports whether the variable has changed since it was last asked.
    fn changed(&mut self) -> bool;

    /// Dumps statistics about the variable internals, for debug and profiling purposes.
    fn dump_stats(&self, round: u32, w: &mut dyn Write);
}

/// An monotonically increasing set of `Tuple`s.
///
/// There are three stages in the lifecycle of a tuple:
///
///   1. A tuple is added to `self.to_add`, but is not yet visible externally.
///   2. Newly added tuples are then promoted to `self.recent` for one iteration.
///   3. After one iteration, recent tuples are moved to `self.tuples` for posterity.
///
/// Each time `self.changed()` is called, the `recent` relation is folded into `tuples`,
/// and the `to_add` relations are merged, potentially deduplicated against `tuples`, and
/// then made  `recent`. This way, across calls to `changed()` all added tuples are in
/// `recent` at least once and eventually all are in `tuples`.
///
/// A `Variable` may optionally be instructed not to de-duplicate its tuples, for reasons
/// of performance. Such a variable cannot be relied on to terminate iterative computation,
/// and it is important that any cycle of derivations have at least one de-duplicating
/// variable on it.
pub struct Variable<Tuple: Ord> {
    /// Should the variable be maintained distinctly.
    pub(crate) distinct: bool,
    /// A useful name for the variable.
    pub(crate) name: String,
    /// A list of relations whose union are the accepted tuples.
    pub stable: Rc<RefCell<Vec<Relation<Tuple>>>>,
    /// A list of recent tuples, still to be processed.
    pub recent: Rc<RefCell<Relation<Tuple>>>,
    /// A list of future tuples, to be introduced.
    pub(crate) to_add: Rc<RefCell<Vec<Relation<Tuple>>>>,
}

// Operator implementations.
impl<Tuple: Ord> Variable<Tuple> {
    /// Adds tuples that result from joining `input1` and `input2` --
    /// each of the inputs must be a set of (Key, Value) tuples. Both
    /// `input1` and `input2` must have the same type of key (`K`) but
    /// they can have distinct value types (`V1` and `V2`
    /// respectively). The `logic` closure will be invoked for each
    /// key that appears in both inputs; it is also given the two
    /// values, and from those it should construct the resulting
    /// value.
    ///
    /// Note that `input1` must be a variable, but `input2` can be a
    /// relation or a variable. Therefore, you cannot join two
    /// relations with this method. This is not because the result
    /// would be wrong, but because it would be inefficient: the
    /// result from such a join cannot vary across iterations (as
    /// relations are fixed), so you should prefer to invoke `insert`
    /// on a relation created by `Relation::from_join` instead.
    ///
    /// # Examples
    ///
    /// This example starts a collection with the pairs (x, x+1) and (x+1, x) for x in 0 .. 10.
    /// It then adds pairs (y, z) for which (x, y) and (x, z) are present. Because the initial
    /// pairs are symmetric, this should result in all pairs (x, y) for x and y in 0 .. 11.
    ///
    /// ```
    /// use datafrog::{Iteration, Relation};
    ///
    /// let mut iteration = Iteration::new();
    /// let variable = iteration.variable::<(usize, usize)>("source");
    /// variable.extend((0 .. 10).map(|x| (x, x + 1)));
    /// variable.extend((0 .. 10).map(|x| (x + 1, x)));
    ///
    /// while iteration.changed() {
    ///     variable.from_join(&variable, &variable, |&key, &val1, &val2| (val1, val2));
    /// }
    ///
    /// let result = variable.complete();
    /// assert_eq!(result.len(), 121);
    /// ```
    pub fn from_join<'me, K: Ord, V1: Ord, V2: Ord>(
        &self,
        input1: &'me Variable<(K, V1)>,
        input2: impl JoinInput<'me, (K, V2)>,
        logic: impl FnMut(&K, &V1, &V2) -> Tuple,
    ) {
        join::join_into(input1, input2, self, logic)
    }

    /// Adds tuples from `input1` whose key is not present in `input2`.
    ///
    /// Note that `input1` must be a variable: if you have a relation
    /// instead, you can use `Relation::from_antijoin` and then
    /// `Variable::insert`.  Note that the result will not vary during
    /// the iteration.
    ///
    /// # Examples
    ///
    /// This example starts a collection with the pairs (x, x+1) for x in 0 .. 10. It then
    /// adds any pairs (x+1,x) for which x is not a multiple of three. That excludes four
    /// pairs (for 0, 3, 6, and 9) which should leave us with 16 total pairs.
    ///
    /// ```
    /// use datafrog::{Iteration, Relation};
    ///
    /// let mut iteration = Iteration::new();
    /// let variable = iteration.variable::<(usize, usize)>("source");
    /// variable.extend((0 .. 10).map(|x| (x, x + 1)));
    ///
    /// let relation: Relation<_> = (0 .. 10).filter(|x| x % 3 == 0).collect();
    ///
    /// while iteration.changed() {
    ///     variable.from_antijoin(&variable, &relation, |&key, &val| (val, key));
    /// }
    ///
    /// let result = variable.complete();
    /// assert_eq!(result.len(), 16);
    /// ```
    pub fn from_antijoin<K: Ord, V: Ord>(
        &self,
        input1: &Variable<(K, V)>,
        input2: &Relation<K>,
        logic: impl FnMut(&K, &V) -> Tuple,
    ) {
        self.insert(join::antijoin(input1, input2, logic))
    }

    /// Adds tuples that result from mapping `input`.
    ///
    /// # Examples
    ///
    /// This example starts a collection with the pairs (x, x) for x in 0 .. 10. It then
    /// repeatedly adds any pairs (x, z) for (x, y) in the collection, where z is the Collatz
    /// step for y: it is y/2 if y is even, and 3*y + 1 if y is odd. This produces all of the
    /// pairs (x, y) where x visits y as part of its Collatz journey.
    ///
    /// ```
    /// use datafrog::{Iteration, Relation};
    ///
    /// let mut iteration = Iteration::new();
    /// let variable = iteration.variable::<(usize, usize)>("source");
    /// variable.extend((0 .. 10).map(|x| (x, x)));
    ///
    /// while iteration.changed() {
    ///     variable.from_map(&variable, |&(key, val)|
    ///         if val % 2 == 0 {
    ///             (key, val/2)
    ///         }
    ///         else {
    ///             (key, 3*val + 1)
    ///         });
    /// }
    ///
    /// let result = variable.complete();
    /// assert_eq!(result.len(), 74);
    /// ```
    pub fn from_map<T2: Ord>(&self, input: &Variable<T2>, logic: impl FnMut(&T2) -> Tuple) {
        map::map_into(input, self, logic)
    }

    /// Adds tuples that result from combining `source` with the
    /// relations given in `leapers`. This operation is very flexible
    /// and can be used to do a combination of joins and anti-joins.
    /// The main limitation is that the things being combined must
    /// consist of one dynamic variable (`source`) and then several
    /// fixed relations (`leapers`).
    ///
    /// The idea is as follows:
    ///
    /// - You will be inserting new tuples that result from joining (and anti-joining)
    ///   some dynamic variable `source` of source tuples (`SourceTuple`)
    ///   with some set of values (of type `Val`).
    /// - You provide these values by combining `source` with a set of leapers
    ///   `leapers`, each of which is derived from a fixed relation. The `leapers`
    ///   should be either a single leaper (of suitable type) or else a tuple of leapers.
    ///   You can create a leaper in one of two ways:
    ///   - Extension: In this case, you have a relation of type `(K, Val)` for some
    ///     type `K`. You provide a closure that maps from `SourceTuple` to the key
    ///     `K`. If you use `relation.extend_with`, then any `Val` values the
    ///     relation provides will be added to the set of values; if you use
    ///     `extend_anti`, then the `Val` values will be removed.
    ///   - Filtering: In this case, you have a relation of type `K` for some
    ///     type `K` and you provide a closure that maps from `SourceTuple` to
    ///     the key `K`. Filters don't provide values but they remove source
    ///     tuples.
    /// - Finally, you get a callback `logic` that accepts each `(SourceTuple, Val)`
    ///   that was successfully joined (and not filtered) and which maps to the
    ///   type of this variable.
    pub fn from_leapjoin<'leap, SourceTuple: Ord, Val: Ord + 'leap>(
        &self,
        source: &Variable<SourceTuple>,
        leapers: impl Leapers<'leap, SourceTuple, Val>,
        logic: impl FnMut(&SourceTuple, &Val) -> Tuple,
    ) {
        self.insert(treefrog::leapjoin(&source.recent.borrow(), leapers, logic));
    }
}

impl<Tuple: Ord> Clone for Variable<Tuple> {
    fn clone(&self) -> Self {
        Variable {
            distinct: self.distinct,
            name: self.name.clone(),
            stable: self.stable.clone(),
            recent: self.recent.clone(),
            to_add: self.to_add.clone(),
        }
    }
}

impl<Tuple: Ord> Variable<Tuple> {
    pub(crate) fn new(name: &str) -> Self {
        Variable {
            distinct: true,
            name: name.to_string(),
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Inserts a relation into the variable.
    ///
    /// This is most commonly used to load initial values into a variable.
    /// it is not obvious that it should be commonly used otherwise, but
    /// it should not be harmful.
    pub fn insert(&self, relation: Relation<Tuple>) {
        if !relation.is_empty() {
            self.to_add.borrow_mut().push(relation);
        }
    }

    /// Extend the variable with values from the iterator.
    ///
    /// This is most commonly used to load initial values into a variable.
    /// it is not obvious that it should be commonly used otherwise, but
    /// it should not be harmful.
    pub fn extend<T>(&self, iterator: impl IntoIterator<Item = T>)
    where
        Relation<Tuple>: FromIterator<T>,
    {
        self.insert(iterator.into_iter().collect());
    }

    /// Consumes the variable and returns a relation.
    ///
    /// This method removes the ability for the variable to develop, and
    /// flattens all internal tuples down to one relation. The method
    /// asserts that iteration has completed, in that `self.recent` and
    /// `self.to_add` should both be empty.
    pub fn complete(self) -> Relation<Tuple> {
        assert!(self.recent.borrow().is_empty());
        assert!(self.to_add.borrow().is_empty());
        let mut result: Relation<Tuple> = Vec::new().into();
        while let Some(batch) = self.stable.borrow_mut().pop() {
            result = result.merge(batch);
        }
        result
    }
}

impl<Tuple: Ord> VariableTrait for Variable<Tuple> {
    fn changed(&mut self) -> bool {
        // 1. Merge self.recent into self.stable.
        if !self.recent.borrow().is_empty() {
            let mut recent =
                ::std::mem::replace(&mut (*self.recent.borrow_mut()), Vec::new().into());
            while self
                .stable
                .borrow()
                .last()
                .map(|x| x.len() <= 2 * recent.len())
                == Some(true)
            {
                let last = self.stable.borrow_mut().pop().unwrap();
                recent = recent.merge(last);
            }
            self.stable.borrow_mut().push(recent);
        }

        // 2. Move self.to_add into self.recent.
        let to_add = self.to_add.borrow_mut().pop();
        if let Some(mut to_add) = to_add {
            while let Some(to_add_more) = self.to_add.borrow_mut().pop() {
                to_add = to_add.merge(to_add_more);
            }
            // 2b. Restrict `to_add` to tuples not in `self.stable`.
            if self.distinct {
                for batch in self.stable.borrow().iter() {
                    let mut slice = &batch[..];
                    // Only gallop if the slice is relatively large.
                    if slice.len() > 4 * to_add.elements.len() {
                        to_add.elements.retain(|x| {
                            slice = join::gallop(slice, |y| y < x);
                            slice.is_empty() || &slice[0] != x
                        });
                    } else {
                        to_add.elements.retain(|x| {
                            while !slice.is_empty() && &slice[0] < x {
                                slice = &slice[1..];
                            }
                            slice.is_empty() || &slice[0] != x
                        });
                    }
                }
            }
            *self.recent.borrow_mut() = to_add;
        }

        // let mut total = 0;
        // for tuple in self.stable.borrow().iter() {
        //     total += tuple.len();
        // }

        // println!("Variable\t{}\t{}\t{}", self.name, total, self.recent.borrow().len());

        !self.recent.borrow().is_empty()
    }

    fn dump_stats(&self, round: u32, w: &mut dyn Write) {
        let mut stable_count = 0;
        for tuple in self.stable.borrow().iter() {
            stable_count += tuple.len();
        }

        writeln!(
            w,
            "{:?},{},{},{}",
            self.name,
            round,
            stable_count,
            self.recent.borrow().len()
        )
        .unwrap_or_else(|e| {
            panic!(
                "Couldn't write stats for variable {}, round {}: {}",
                self.name, round, e
            )
        });
    }
}

// impl<Tuple: Ord> Drop for Variable<Tuple> {
//     fn drop(&mut self) {
//         let mut total = 0;
//         for batch in self.stable.borrow().iter() {
//             total += batch.len();
//         }
//         println!("FINAL: {:?}\t{:?}", self.name, total);
//     }
// }

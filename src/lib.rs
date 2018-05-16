//! A lightweight Datalog engine in Rust
//!
//! The intended design is that one has static `Relation` types that are sets
//! of tuples, and `Variable` types that represent monotonically increasing
//! sets of tuples.
//!
//! The types are mostly wrappers around `Vec<Tuple>` indicating sorted-ness,
//! and the intent is that this code can be dropped in the middle of an otherwise
//! normal Rust program, run to completion, and then the results extracted as
//! vectors again.

#![forbid(missing_docs)]

use std::rc::Rc;
use std::cell::RefCell;

mod map;
mod join;

/// A static, ordered list of key-value pairs.
///
/// A relation represents a fixed set of key-value pairs. In many places in a
/// Datalog computation we want to be sure that certain relations are not able
/// to vary (for example, in antijoins).
pub struct Relation<Tuple: Ord> {
    /// Wrapped elements in the relation.
    ///
    /// It is crucial that if this type is constructed manually, this field be
    /// sorted, and it is probably important that all elements be distinct.
    pub elements: Vec<Tuple>
}

impl<Tuple: Ord> Relation<Tuple> {
    /// Merges two relations into their union.
    pub fn merge(mut self, mut other: Self) -> Self {
        let mut vec = Vec::with_capacity(self.len() + other.len());
        vec.extend(self.elements.drain(..));
        vec.extend(other.elements.drain(..));
        vec.sort();
        vec.dedup();
        Relation {
            elements: vec
        }
    }
}

impl<Tuple: Ord> From<Vec<Tuple>> for Relation<Tuple> {
    fn from(mut elements: Vec<Tuple>) -> Self {
        elements.sort_unstable();
        Relation { elements }
    }
}

impl<Tuple: Ord> std::ops::Deref for Relation<Tuple> {
    type Target = [Tuple];
    fn deref(&self) -> &Self::Target {
        &self.elements[..]
    }
}

/// An iterative context for recursive evaluation.
///
/// An `Iteration` tracks monotonic variables, and monitors their progress.
/// It can inform the user if they have ceased changing, at which point the
/// computation should be done.
pub struct Iteration {
    variables: Vec<Box<VariableTrait>>,
}

impl Iteration {
    /// Create a new iterative context.
    pub fn new() -> Self {
        Iteration { variables: Vec::new() }
    }
    /// Reports whether any of the monitored variables have changed since
    /// the most recent call.
    pub fn changed(&mut self) -> bool {
        let mut result = false;
        for variable in self.variables.iter_mut() {
            if variable.changed() { result = true; }
        }
        result
    }
    /// Creates a new named variable associated with the iterative context.
    pub fn variable<Tuple: Ord+'static>(&mut self, name: &str) -> Variable<Tuple> {
        let variable = Variable::new(name);
        self.variables.push(Box::new(variable.clone()));
        variable
    }
}

/// A type that can report on whether it has changed.
pub trait VariableTrait {
    /// Reports whether the variable has changed since it was last asked.
    fn changed(&mut self) -> bool;
}

/// An monotonically increasing set of `Tuple`s.
///
/// The design here is that there are three types of tuples: i. those that have been
/// processed by all operators that can access the variable, ii. those that should now
/// be processed by all operators that can access the variable, and iii. those that
/// have only just been added and should eventually be promoted to type ii. (but which
/// are currently hidden).
///
/// Each time `self.changed()` is called, the `recent` relation is folded into `tuples`,
/// and the `to_add` relations are merged, deduplicated against `tuples`, and then made
/// `recent`. This way, across calls to `changed()` all added relations are at some point
/// in `recent` once and eventually all are in `tuples`.
pub struct Variable<Tuple: Ord> {
    /// A useful name for the variable.
    pub name: String,
    /// A list of relations whose union are the accepted tuples.
    pub tuples: Rc<RefCell<Vec<Relation<Tuple>>>>,
    /// A list of recent tuples, still to be processed.
    pub recent: Rc<RefCell<Relation<Tuple>>>,
    /// A list of future tuples, to be introduced.
    pub to_add: Rc<RefCell<Vec<Relation<Tuple>>>>,
}

impl<Tuple: Ord> Variable<Tuple> {
    /// Adds tuples that result from joining `input1` and `input2`.
    pub fn from_join<K: Ord,V1: Ord, V2: Ord, F: Fn(&K,&V1,&V2)->Tuple>(
        &self,
        input1: &Variable<(K,V1)>,
        input2: &Variable<(K,V2)>,
        logic: F)
    {
        join::join_into(input1, input2, self, logic)
    }
    /// Adds tuples that result from antijoining `input1` and `input2`.
    pub fn from_antijoin<K: Ord,V: Ord, F: Fn(&K,&V)->Tuple>(
        &self,
        input1: &Variable<(K,V)>,
        input2: &Relation<K>,
        logic: F)
    {
        join::antijoin_into(input1, input2, self, logic)
    }
    /// Adds tuples that result from mapping `input`.
    pub fn from_map<T2: Ord, F: Fn(&T2)->Tuple>(&self, input: &Variable<T2>, logic: F) {
        map::map_into(input, self, logic)
    }
}

impl<Tuple: Ord> Clone for Variable<Tuple> {
    fn clone(&self) -> Self {
        Variable {
            name: self.name.clone(),
            tuples: self.tuples.clone(),
            recent: self.recent.clone(),
            to_add: self.to_add.clone(),
        }
    }
}

impl<Tuple: Ord> Variable<Tuple> {
    fn new(name: &str) -> Self {
        Variable {
            name: name.to_string(),
            tuples: Rc::new(RefCell::new(Vec::new().into())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new().into())),
        }
    }
    /// Inserts a relation into the variable.
    ///
    /// This is most commonly used to load initial values into a variable.
    /// it is not obvious that it should be commonly used otherwise, but
    /// it should not be harmful.
    pub fn insert(&self, relation: Relation<Tuple>) {
        self.to_add.borrow_mut().push(relation);
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
        while let Some(batch) = self.tuples.borrow_mut().pop() {
            result = result.merge(batch);
        }
        result
    }
}

impl<Tuple: Ord> VariableTrait for Variable<Tuple> {
    fn changed(&mut self) -> bool {

        // 1. Merge self.recent into self.tuples.
        let mut recent = ::std::mem::replace(&mut (*self.recent.borrow_mut()), Vec::new().into());
        while self.tuples.borrow().last().map(|x| x.len() <= 2 * recent.len()) == Some(true) {
            let last = self.tuples.borrow_mut().pop().unwrap();
            recent = recent.merge(last);
        }
        self.tuples.borrow_mut().push(recent);

        // 2. Move self.to_add into self.recent.
        let to_add = self.to_add.borrow_mut().pop();
        if let Some(mut to_add) = to_add {
            while let Some(to_add_more) = self.to_add.borrow_mut().pop() {
                to_add = to_add.merge(to_add_more);
            }
            // 2b. Restrict `to_add` to tuples not in `self.tuples`.
            for batch in self.tuples.borrow().iter() {
                let mut slice = &batch[..];
                to_add.elements.retain(|x| {
                    slice = join::gallop(slice, |y| y < x);
                    slice.len() == 0 || &slice[0] != x
                })
            }
            *self.recent.borrow_mut() = to_add;
        }

        let mut total = 0;
        for tuple in self.tuples.borrow().iter() {
            total += tuple.len();
        }

        println!("Variable\t{}\t{}\t{}", self.name, total, self.recent.borrow().len());

        !self.recent.borrow().is_empty()
    }
}
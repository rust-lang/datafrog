//! A lightweight Datalog executor in Rust
//!
//! The intended design is that there are static relations, perhaps described
//! by sorted `Vec<(Key, Val)>` lists, at which point you can make an iterative
//! context in which you can name variables, define computations, and bind the
//! results to named variables.
//!

use std::rc::Rc;
use std::cell::RefCell;

/// A static, ordered list of key-value pairs.
///
/// A relation represents a fixed set of key-value pairs. In many places in a
/// Datalog computation we want to be sure that certain relations are not able
/// to vary (for example, in antijoins).
pub struct Relation<Tuple: Ord> {
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
pub struct Variable<Tuple: Ord> {
    pub name: String,
    pub tuples: Rc<RefCell<Vec<Relation<Tuple>>>>,
    pub recent: Rc<RefCell<Relation<Tuple>>>,
    pub to_add: Rc<RefCell<Vec<Relation<Tuple>>>>,
}

impl<Tuple: Ord> Variable<Tuple> {
    pub fn from_join<K: Ord,V1: Ord, V2: Ord, F: Fn(&K,&V1,&V2)->Tuple>(
        &self,
        input1: &Variable<(K,V1)>,
        input2: &Variable<(K,V2)>,
        logic: F)
    {
        join::join_into(input1, input2, self, logic)
    }
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
    pub fn insert(&self, relation: Relation<Tuple>) {
        self.to_add.borrow_mut().push(relation);
    }
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


// fn antijoin_into<Key: Ord, Val: Ord>(
//     input1: &Variable<(Key, Val)>,
//     input2: &Relation<Key>,
//     output: &Variable<Result>) {

//     let mut results = Vec::new();

//     // Scoped so that we don't write to `output` while holding any borrows.
//     {
//         // Read-only access to each variable.
//         let tuples1 = input1.tuples.borrow();
//         let tuples2 = input2.tuples.borrow();

//         let recent1 = input1.recent.borrow();
//         let recent2 = input2.recent.borrow();

//         // Iterate through each new input1 batch.
//         for &(ref key1, ref val1) in recent1.iter() {
//             for batch2 in tuples2.iter() {
//                 for &(ref key2, ref val2) in batch2.iter() {
//                     if key1 == key2 {
//                         results.push(key1, val1, val2));
//                     }
//                 }
//             }
//         }
//     }

//     output.insert(results.into());
// }

// fn main() {

//     let mut nodes = Vec::new();
//     let mut edges = Vec::new();

//     use std::io::{BufRead, BufReader};
//     use std::fs::File;

//     let filename = std::env::args().nth(1).unwrap();
//     let file = BufReader::new(File::open(filename).unwrap());
//     for readline in file.lines() {
//         let line = readline.ok().expect("read error");
//         if !line.starts_with('#') && line.len() > 0 {
//             let mut elts = line[..].split_whitespace();
//             let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
//             let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
//             let typ: &str = elts.next().unwrap();
//             match typ {
//                 "n" => { nodes.push((dst, src)); },
//                 "e" => { edges.push((src, dst)); },
//                 unk => { panic!("unknown type: {}", unk)},
//             }
//         }
//     }

//     let mut iteration = Iteration::new();

//     let variable1 = iteration.variable::<(u32,u32)>("nodes");
//     let variable2 = iteration.variable::<(u32,u32)>("edges");

//     variable1.insert(nodes.into());
//     variable2.insert(edges.into());

//     while iteration.changed() {

//         // N(a,c) <-  N(a,b), E(b,c)
//         variable1.from_join(&variable1, &variable2, |_b, &a, &c| (c,a));

//     }

//     let reachable = variable1.complete();

// }

mod map {

    use super::Variable;

    pub fn map_into<T1: Ord, T2: Ord, F: Fn(&T1)->T2>(
        input: &Variable<T1>,
        output: &Variable<T2>,
        logic: F) {

        let mut results = Vec::new();
        let recent = input.recent.borrow();
        for tuple in recent.iter() {
            results.push(logic(tuple));
        }

        output.insert(results.into());
}

}

mod join {

    use super::Variable;

    pub fn join_into<Key: Ord, Val1: Ord, Val2: Ord, Result: Ord, F: Fn(&Key, &Val1, &Val2)->Result>(
        input1: &Variable<(Key, Val1)>,
        input2: &Variable<(Key, Val2)>,
        output: &Variable<Result>,
        logic: F) {

        let mut results = Vec::new();

        // Scoped so that we don't write to `output` while holding any borrows.
        {
            // Read-only access to each variable.
            let tuples1 = input1.tuples.borrow();
            let tuples2 = input2.tuples.borrow();

            let recent1 = input1.recent.borrow();
            let recent2 = input2.recent.borrow();

            for batch2 in tuples2.iter() {
                join_helper(&recent1, &batch2, |k,v1,v2| results.push(logic(k,v1,v2)))
            }

            for batch1 in tuples1.iter() {
                join_helper(&batch1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)))
            }

            join_helper(&recent1, &recent2, |k,v1,v2| results.push(logic(k,v1,v2)))
        }

        output.insert(results.into());
    }

    fn join_helper<K: Ord, V1, V2, F: FnMut(&K, &V1, &V2)>(mut slice1: &[(K,V1)], mut slice2: &[(K,V2)], mut result: F) {

        while !slice1.is_empty() && !slice2.is_empty() {

            if slice1[0].0 == slice2[0].0 {

                let mut key1_count = 0;
                while key1_count < slice1.len() && slice1[0].0 == slice1[key1_count].0 {
                    key1_count += 1;
                }

                let mut key2_count = 0;
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
}
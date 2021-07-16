use std::io::Write;

use crate::variable::{Variable, VariableTrait};

/// An iterative context for recursive evaluation.
///
/// An `Iteration` tracks monotonic variables, and monitors their progress.
/// It can inform the user if they have ceased changing, at which point the
/// computation should be done.
#[derive(Default)]
pub struct Iteration {
    variables: Vec<Box<dyn VariableTrait>>,
    round: u32,
    debug_stats: Option<Box<dyn Write>>,
}

impl Iteration {
    /// Create a new iterative context.
    pub fn new() -> Self {
        Self::default()
    }
    /// Reports whether any of the monitored variables have changed since
    /// the most recent call.
    pub fn changed(&mut self) -> bool {
        self.round += 1;

        let mut result = false;
        for variable in self.variables.iter_mut() {
            if variable.changed() {
                result = true;
            }

            if let Some(ref mut stats_writer) = self.debug_stats {
                variable.dump_stats(self.round, stats_writer);
            }
        }
        result
    }
    /// Creates a new named variable associated with the iterative context.
    pub fn variable<Tuple: Ord + 'static>(&mut self, name: &str) -> Variable<Tuple> {
        let variable = Variable::new(name);
        self.variables.push(Box::new(variable.clone()));
        variable
    }
    /// Creates a new named variable associated with the iterative context.
    ///
    /// This variable will not be maintained distinctly, and may advertise tuples as
    /// recent multiple times (perhaps unboundedly many times).
    pub fn variable_indistinct<Tuple: Ord + 'static>(&mut self, name: &str) -> Variable<Tuple> {
        let mut variable = Variable::new(name);
        variable.distinct = false;
        self.variables.push(Box::new(variable.clone()));
        variable
    }

    /// Set up this Iteration to write debug statistics about each variable,
    /// for each round of the computation.
    pub fn record_stats_to(&mut self, mut w: Box<dyn Write>) {
        // print column names header
        writeln!(w, "Variable,Round,Stable count,Recent count")
            .expect("Couldn't write debug stats CSV header");

        self.debug_stats = Some(w);
    }
}

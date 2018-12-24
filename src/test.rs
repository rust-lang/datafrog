#![cfg(test)]

use crate::Relation;
use crate::Iteration;
use crate::RelationLeaper;
use proptest::prelude::*;
use proptest::{proptest, proptest_helper};

fn inputs() -> impl Strategy<Value = Vec<(u32, u32)>> {
    prop::collection::vec((0_u32..100, 0_u32..100), 1..100)
}

fn reachable_with_join(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges = Relation::from(edges.iter().cloned());
    let mut iteration = Iteration::new();

    let edges_by_successor = iteration.variable::<(u32, u32)>("edges_invert");
    edges_by_successor.insert(Relation::from(edges.iter().map(|&(n1, n2)| (n2, n1))));

    let reachable = iteration.variable::<(u32, u32)>("reachable");
    reachable.insert(edges);

    while iteration.changed() {
        // reachable(N1, N3) :- edges(N1, N2), reachable(N2, N3).
        reachable.from_join(&reachable, &edges_by_successor, |&_, &n3, &n1| (n1, n3));
    }

    reachable.complete()
}

fn reachable_with_leapfrog(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges = Relation::from(edges.iter().cloned());
    let mut iteration = Iteration::new();

    let edges_by_successor = Relation::from(edges.iter().map(|&(n1, n2)| (n2, n1)));

    let reachable = iteration.variable::<(u32, u32)>("reachable");
    reachable.insert(edges);

    while iteration.changed() {
        // reachable(N1, N3) :- edges(N1, N2), reachable(N2, N3).
        reachable.from_leapjoin(
            &reachable,
            &mut [
                &mut edges_by_successor.extend_with(|&(n2, _)| n2),
            ],
            |&(_, n3), &n1| (n1, n3),
        );
    }

    reachable.complete()
}

proptest! {
    #[test]
    fn reachable(edges in inputs()) {
        let reachable1 = reachable_with_join(&edges);
        let reachable2 = reachable_with_leapfrog(&edges);
        assert_eq!(reachable1.elements, reachable2.elements);
    }
}

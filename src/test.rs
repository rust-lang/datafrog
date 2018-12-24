#![cfg(test)]

use crate::Iteration;
use crate::Relation;
use crate::RelationLeaper;
use proptest::prelude::*;
use proptest::{proptest, proptest_helper};

fn inputs() -> impl Strategy<Value = Vec<(u32, u32)>> {
    prop::collection::vec((0_u32..100, 0_u32..100), 1..500)
}

/// The original way to use datafrog -- computes reachable nodes from a set of edges
fn reachable_with_var_join(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
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

/// Like `reachable`, but using a relation as an input to `from_join`
fn reachable_with_relation_join(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges: Relation<_> = edges.iter().collect();
    let mut iteration = Iteration::new();

    // NB. Changed from `reachable_with_var_join`:
    let edges_by_successor = Relation::from(edges.iter().map(|&(n1, n2)| (n2, n1)));

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
            &mut [&mut edges_by_successor.extend_with(|&(n2, _)| n2)],
            |&(_, n3), &n1| (n1, n3),
        );
    }

    reachable.complete()
}

/// Computes a join where the values are summed -- uses iteration
/// variables (the original datafrog technique).
fn sum_join_via_var(
    input1_slice: &[(u32, u32)],
    input2_slice: &[(u32, u32)],
) -> Relation<(u32, u32)> {
    let mut iteration = Iteration::new();

    let input1 = iteration.variable::<(u32, u32)>("input1");
    input1.insert(Relation::from(input1_slice.iter().cloned()));

    let input2 = iteration.variable::<(u32, u32)>("input1");
    input2.insert(Relation::from(input2_slice.iter().cloned()));

    let output = iteration.variable::<(u32, u32)>("output");

    while iteration.changed() {
        // output(K1, V1 * 100 + V2) :- input1(K1, V1), input2(K1, V2).
        output.from_join(&input1, &input2, |&k1, &v1, &v2| (k1, v1 * 100 + v2));
    }

    output.complete()
}

/// Computes a join where the values are summed -- uses iteration
/// variables (the original datafrog technique).
fn sum_join_via_relation(
    input1_slice: &[(u32, u32)],
    input2_slice: &[(u32, u32)],
) -> Relation<(u32, u32)> {
    let input1 = Relation::from(input1_slice.iter().cloned());
    let input2 = Relation::from(input2_slice.iter().cloned());
    Relation::from_join(&input1, &input2, |&k1, &v1, &v2| (k1, v1 * 100 + v2))
}

proptest! {
    #[test]
    fn reachable_leapfrog_vs_var_join(edges in inputs()) {
        let reachable1 = reachable_with_var_join(&edges);
        let reachable2 = reachable_with_leapfrog(&edges);
        assert_eq!(reachable1.elements, reachable2.elements);
    }

    #[test]
    fn reachable_rel_join_vs_var_join(edges in inputs()) {
        let reachable1 = reachable_with_var_join(&edges);
        let reachable2 = reachable_with_relation_join(&edges);
        assert_eq!(reachable1.elements, reachable2.elements);
    }

    #[test]
    fn sum_join_from_var_vs_rel((set1, set2) in (inputs(), inputs())) {
        let output1 = sum_join_via_var(&set1, &set2);
        let output2 = sum_join_via_relation(&set1, &set2);
        assert_eq!(output1.elements, output2.elements);
    }
}

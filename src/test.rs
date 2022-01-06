#![cfg(test)]

use crate::Iteration;
use crate::Relation;
use proptest::prelude::*;
use proptest::{proptest, proptest_helper};

fn inputs() -> impl Strategy<Value = Vec<(u32, u32)>> {
    prop::collection::vec((0_u32..100, 0_u32..100), 1..500)
}

/// The original way to use datafrog -- computes reachable nodes from a set of edges
fn reachable_with_var_join(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges: Relation<(u32, u32)> = edges.iter().collect();
    let mut iteration = Iteration::new();

    let edges_by_successor = iteration.variable::<(u32, u32)>("edges_invert");
    edges_by_successor.extend(edges.iter().map(|&(n1, n2)| (n2, n1)));

    let reachable = iteration.variable::<(u32, u32)>("reachable");
    reachable.insert(edges);

    while iteration.changed() {
        // reachable(N1, N3) :- edges(N1, N2), reachable(N2, N3).
        reachable.from_join_first(&reachable, &edges_by_successor, |_, n3, n1| (n1, n3));
    }

    reachable.complete()
}

/// Like `reachable`, but using a relation as an input to `from_join`
fn reachable_with_relation_join(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges: Relation<(u32, u32)> = edges.iter().collect();
    let mut iteration = Iteration::new();

    // NB. Changed from `reachable_with_var_join`:
    let edges_by_successor: Relation<_> = edges.iter().map(|&(n1, n2)| (n2, n1)).collect();

    let reachable = iteration.variable::<(u32, u32)>("reachable");
    reachable.insert(edges);

    while iteration.changed() {
        // reachable(N1, N3) :- edges(N1, N2), reachable(N2, N3).
        reachable.from_join_first(&reachable, &edges_by_successor, |_, n3, n1| (n1, n3));
    }

    reachable.complete()
}

fn reachable_with_leapfrog(edges: &[(u32, u32)]) -> Relation<(u32, u32)> {
    let edges: Relation<_> = edges.iter().collect();
    let mut iteration = Iteration::new();

    let edges_by_successor: Relation<_> = edges.iter().map(|&(n1, n2)| (n2, n1)).collect();

    let reachable = iteration.variable::<(u32, u32)>("reachable");
    reachable.insert(edges);

    while iteration.changed() {
        // reachable(N1, N3) :- edges(N1, N2), reachable(N2, N3).
        reachable.from_leapjoin(
            &reachable,
            edges_by_successor.extend_with(|&(n2, _)| (n2,)),
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
    input1.extend(input1_slice);

    let input2 = iteration.variable::<(u32, u32)>("input1");
    input2.extend(input2_slice);

    let output = iteration.variable::<(u32, u32)>("output");

    while iteration.changed() {
        // output(K1, V1 * 100 + V2) :- input1(K1, V1), input2(K1, V2).
        output.from_join_first(&input1, &input2, |k1, v1, v2| (k1, v1 * 100 + v2));
    }

    output.complete()
}

/// Computes a join where the values are summed -- uses iteration
/// variables (the original datafrog technique).
fn sum_join_via_relation(
    input1_slice: &[(u32, u32)],
    input2_slice: &[(u32, u32)],
) -> Relation<(u32, u32)> {
    let input1: Relation<(u32, u32)> = input1_slice.iter().collect();
    let input2: Relation<(u32, u32)> = input2_slice.iter().collect();
    Relation::from_join_first(&input1, &input2, |k1, v1, v2| (k1, v1 * 100 + v2))
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

    /// Test the behavior of `filter_anti` used on its own in a
    /// leapjoin -- effectively it becomes an "intersection"
    /// operation.
    #[test]
    fn filter_with_on_its_own((set1, set2) in (inputs(), inputs())) {
        let input1: Relation<(u32, u32)> = set1.iter().collect();
        let input2: Relation<(u32, u32)> = set2.iter().collect();
        let intersection1 = Relation::from_leapjoin(
            &input1,
            input2.filter_with(|&tuple| tuple),
            |&tuple, &()| tuple,
        );

        let intersection2: Relation<(u32, u32)> = input1.elements.iter()
            .filter(|t| input2.elements.binary_search(&t).is_ok())
            .collect();

        assert_eq!(intersection1.elements, intersection2.elements);
    }

    /// Test the behavior of `filter_anti` used on its own in a
    /// leapjoin -- effectively it becomes a "set minus" operation.
    #[test]
    fn filter_anti_on_its_own((set1, set2) in (inputs(), inputs())) {
        let input1: Relation<(u32, u32)> = set1.iter().collect();
        let input2: Relation<(u32, u32)> = set2.iter().collect();

        let difference1 = Relation::from_leapjoin(
            &input1,
            input2.filter_anti(|&tuple| tuple),
            |&tuple, &()| tuple,
        );

        let difference2: Relation<(u32, u32)> = input1.elements.iter()
            .filter(|t| input2.elements.binary_search(&t).is_err())
            .collect();

        assert_eq!(difference1.elements, difference2.elements);
    }
}

/// Test that `from_leapjoin` matches against the tuples from an
/// `extend` that precedes first iteration.
///
/// This was always true, but wasn't immediately obvious to me until I
/// re-read the code more carefully. -nikomatsakis
#[test]
fn leapjoin_from_extend() {
    let doubles: Relation<(u32, u32)> = (0..10).map(|i| (i, i * 2)).collect();

    let mut iteration = Iteration::new();

    let variable = iteration.variable::<(u32, u32)>("variable");
    variable.extend(Some((2, 2)));

    while iteration.changed() {
        variable.from_leapjoin(
            &variable,
            doubles.extend_with(|&(i, _)| (i,)),
            |&(i, _), &j| (i, j),
        );
    }

    let variable = variable.complete();

    assert_eq!(variable.elements, vec![(2, 2), (2, 4)]);
}

#[test]
fn passthrough_leaper() {
    let mut iteration = Iteration::new();

    let variable = iteration.variable::<(u32, u32)>("variable");
    variable.extend((0..10).map(|i| (i, i)));

    while iteration.changed() {
        variable.from_leapjoin(
            &variable,
            (
                crate::passthrough(), // Without this, the test would fail at runtime.
                crate::PrefixFilter::from(|&(i, _)| i <= 20),
            ),
            |&(i, j), ()| (2*i, 2*j),
        );
    }

    let variable = variable.complete();

    let mut expected: Vec<_> = (0..10).map(|i| (i, i)).collect();
    expected.extend((10..20).filter_map(|i| (i%2 == 0).then(|| (i, i))));
    expected.extend((20..=40).filter_map(|i| (i%4 == 0).then(|| (i, i))));
    assert_eq!(&*variable, &expected);
}

#[test]
fn relation_from_antijoin() {
    let lhs: Relation<(u32, u32)> = (0 .. 10).map(|x| (x, x)).collect();
    let rhs: Relation<(u32,)> = (0 .. 10).filter(|x| x % 2 == 0).map(|x| (x,)).collect();
    let expected: Relation<_> = (0 .. 10).filter(|x| x % 2 == 1).map(|x| (x, x)).collect();

    let result = Relation::from_antijoin(&lhs, &rhs, |x| x);

    assert_eq!(result.elements, expected.elements);
}

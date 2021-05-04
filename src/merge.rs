//! Subroutines for merging sorted lists efficiently.

use std::cmp::Ordering;

/// Merges two sorted lists into a single sorted list, ignoring duplicates.
pub fn merge_unique<T: Ord>(mut a: Vec<T>, mut b: Vec<T>) -> Vec<T> {
    // If one of the lists is zero-length, we don't need to do any work.
    if a.is_empty() {
        return b;
    }
    if b.is_empty() {
        return a;
    }

    // Fast path for when all the new elements are after the existing ones.
    //
    // Cannot panic because we check for empty inputs above.
    if *a.last().unwrap() < b[0] {
        a.append(&mut b);
        return a;
    }
    if *b.last().unwrap() < a[0] {
        b.append(&mut a);
        return b;
    }

    // Ensure that `out` always has sufficient capacity.
    //
    // SAFETY: The calls to `push_unchecked` below are safe because of this.
    let mut out = Vec::with_capacity(a.len() + b.len());

    let mut a = a.into_iter();
    let mut b = b.into_iter();

    // While both inputs have elements remaining, copy the lesser element to the output vector.
    while a.len() != 0 && b.len() != 0 {
        // SAFETY: The following calls to `get_unchecked` and `next_unchecked` are safe because we
        // ensure that `a.len() > 0` and `b.len() > 0` inside the loop.
        //
        // I was hoping to avoid using "unchecked" operations, but it seems the bounds checks
        // don't get optimized away. Using `ExactSizeIterator::is_empty` instead of checking `len`
        // seemed to help, but that method is unstable.

        let a_elem = unsafe { a.as_slice().get_unchecked(0) };
        let b_elem = unsafe { b.as_slice().get_unchecked(0) };
        match a_elem.cmp(b_elem) {
            Ordering::Less => unsafe { push_unchecked(&mut out, next_unchecked(&mut a)) },
            Ordering::Greater => unsafe { push_unchecked(&mut out, next_unchecked(&mut b)) },
            Ordering::Equal => unsafe {
                push_unchecked(&mut out, next_unchecked(&mut a));
                std::mem::drop(next_unchecked(&mut b));
            },
        }
    }

    // Once either `a` or `b` runs out of elements, copy all remaining elements in the other one
    // directly to the back of the output list.
    //
    // This branch is free because we have to check `a.is_empty()` above anyways.
    //
    // Calling `push_unchecked` in a loop was slightly faster than `out.extend(...)`
    // despite the fact that `std::vec::IntoIter` implements `TrustedLen`.
    if a.len() != 0 {
        for elem in a {
            unsafe {
                push_unchecked(&mut out, elem);
            }
        }
    } else {
        for elem in b {
            unsafe {
                push_unchecked(&mut out, elem);
            }
        }
    }

    out
}

/// Pushes `value` to `vec` without checking that the vector has sufficient capacity.
///
/// If `vec.len() == vec.cap()`, calling this function is UB.
unsafe fn push_unchecked<T>(vec: &mut Vec<T>, value: T) {
    let end = vec.as_mut_ptr().add(vec.len());
    std::ptr::write(end, value);
    vec.set_len(vec.len() + 1);
}

/// Equivalent to `iter.next().unwrap()` that is UB to call when `iter` is empty.
unsafe fn next_unchecked<T>(iter: &mut std::vec::IntoIter<T>) -> T {
    match iter.next() {
        Some(x) => x,
        None => std::hint::unreachable_unchecked(),
    }
}

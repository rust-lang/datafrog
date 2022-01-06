/// Trait for splitting a tuple into two at some index.
///
/// `P` is a tuple containing only the desired fields prior to the index. For some type `(A, B)`,
/// `(A, B)`, `(A,)`, and `()` are all valid choices for `P`, but `(B,)` is not. `A` is also not a
/// valid prefix, since `P` must be a tuple.
pub trait Split<P>: Sized {
    /// The remaining fields after the input has been split.
    type Suffix: Sized;

    /// Splits a tuple into a prefix and a suffix.
    fn split(self) -> (P, Self::Suffix);

    /// Returns the prefix of the tuple.
    fn prefix(self) -> P {
        self.split().0
    }

    /// Returns the suffix of the tuple.
    fn suffix(self) -> Self::Suffix {
        self.split().1
    }
}

macro_rules! rev {
    ($($T:ident)*) => {
        rev!(@REV [$($T)*])
    };
    (@REV [$H:ident $($T:ident)*] $($R:ident)*) => {
        rev!(@REV [$($T)*] $H $($R)*)
    };
    (@REV [$H:ident] $($R:ident)*) => {
        rev!(@REV [] $H $($R)*)
    };
    (@REV [] $($R:ident)*) => { ($($R,)*) };
}

macro_rules! impls {
    ($($t:ident)*) => {
        impls!(@outer [$($t)*]);
    };

    (@outer [$h:ident $($t:ident)+]) => {
        impls!(@inner [$h $($t)*] [$h $($t)*] []);
        impls!(@outer [$($t)*]);
    };
    (@outer [$h:ident]) => {
        impls!(@inner [$h] [$h] []);
        impls!(@outer []);
    };
    (@outer []) => {};

    (@inner [$($t:ident)+] [$p:ident $($pt:ident)*] [$($r:ident)*]) => {
        impls!(@imp [$($t)*] [$p $($pt)*] [$($r)*]);
        impls!(@inner [$($t)*] [$($pt)*] [$p $($r)*]);
    };
    (@inner [$($t:ident)+] [$p:ident] [$($r:ident)*]) => {
        impls!(@imp [$($t)*] [$p] [$($r)*]);
        impls!(@inner [$($t)*] [] [$p $($r)*]);
    };
    (@inner [$($t:ident)+] [] [$($r:ident)*]) => {
        impls!(@imp [$($t)*] [] [$($r)*]);
    };

    (@imp [$($t:ident)+] [$($p:ident)*] [$($r:ident)*]) => {
        impl<$($t),*> Split<rev!($($p)*)> for rev!($($t)*) {
            type Suffix = ($($r,)*);

            fn split(self) -> (rev!($($p)*), Self::Suffix) {
                let rev!($($t)*) = self;
                (rev!($($p)*), ($($r,)*))
            }
        }
    };
    (@imp [$($t:ident),+] [] []) => {
        impl<$($t),*> Split<()> for rev!($($t),*) {
            type Suffix = Self;

            fn split(self) -> ((), Self) {
                ((), self)
            }
        }
    };
}

#[allow(non_snake_case)]
mod impls {
    use super::*;

    impls!(F E D C B A);
}

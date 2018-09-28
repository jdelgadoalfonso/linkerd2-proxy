use std::marker::PhantomData;

use super::when;

/// A stackable element.
///
/// Given a `Next`-typed inner value, produces a `Make`-typed value.
/// This is especially useful for composable types like `Make`s.
pub trait Layer<Target, NextTarget, Next: super::Make<NextTarget>> {
    type Value;
    type Error;
    type Make: super::Make<Target, Value = Self::Value, Error = Self::Error>;

    /// Produce a `Make` value from a `Next` value.
    fn bind(&self, next: Next) -> Self::Make;

    /// Compose this `Layer` with another.
    fn and_then<T, N, L>(self, inner: L)
        -> AndThen<Target, NextTarget, T, N, Self, L>
    where
        N: super::Make<T>,
        L: Layer<NextTarget, T, N>,
        Self: Layer<Target, NextTarget, L::Make> + Sized,
    {
        AndThen {
            outer: self,
            inner,
            _p: PhantomData,
        }
    }

    fn and_when<P, N, L>(self, predicate: P, inner: L)
        -> AndThen<Target, NextTarget, NextTarget, N, Self, when::Layer<NextTarget, P, N, L>>
    where
        P: when::Predicate<NextTarget> + Clone,
        N: super::Make<NextTarget> + Clone,
        L: Layer<NextTarget, NextTarget, N, Error = N::Error> + Clone,
        Self: Layer<Target, NextTarget, when::Make<NextTarget, P, N, L>> + Sized,
    {
        AndThen {
            outer: self,
            inner: when::Layer::new(predicate, inner),
            _p: PhantomData,
        }
    }
}

/// Combines two `Layers` into one layer.
#[derive(Debug, Clone)]
pub struct AndThen<A, B, C, Next, Outer, Inner>
where
    Outer: Layer<A, B, Inner::Make>,
    Inner: Layer<B, C, Next>,
    Next: super::Make<C>,
{
    outer: Outer,
    inner: Inner,
    // `AndThen` should be Send/Sync independently of `Next`.
    _p: PhantomData<fn() -> (A, B, C, Next)>,
}

impl<A, B, C, Next, Outer, Inner> Layer<A, C, Next>
    for AndThen<A, B, C, Next, Outer, Inner>
where
    Outer: Layer<A, B, Inner::Make>,
    Inner: Layer<B, C, Next>,
    Next: super::Make<C>,
{
    type Value = Outer::Value;
    type Error = Outer::Error;
    type Make = Outer::Make;

    fn bind(&self, next: Next) -> Self::Make {
        self.outer.bind(self.inner.bind(next))
    }
}

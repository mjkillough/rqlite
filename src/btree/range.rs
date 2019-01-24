use std::cmp::Ordering;
use std::marker::PhantomData;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RangeComparison {
    Less,
    InRange,
    UpperBoundary,
    Greater,
}

pub trait Range {
    type Key;

    fn compare(&self, key: &Self::Key) -> RangeComparison;
}

#[derive(Copy, Clone, Debug)]
pub struct RangeAll<K>(PhantomData<K>);

impl<K> RangeAll<K> {
    pub fn new() -> RangeAll<K> {
        RangeAll(PhantomData)
    }
}

impl<K> Range for RangeAll<K> {
    type Key = K;

    fn compare(&self, _key: &Self::Key) -> RangeComparison {
        RangeComparison::InRange
    }
}

pub struct RangeOne<K: Ord>(K);

impl<K: Ord> RangeOne<K> {
    pub fn new(key: K) -> RangeOne<K> {
        RangeOne(key)
    }
}

impl<K: Ord> Range for RangeOne<K> {
    type Key = K;

    fn compare(&self, key: &Self::Key) -> RangeComparison {
        match key.cmp(&self.0) {
            Ordering::Less => RangeComparison::Less,
            Ordering::Equal => RangeComparison::UpperBoundary,
            Ordering::Greater => RangeComparison::Greater,
        }
    }
}

pub struct RangeGtEq<K: Ord>(K);

impl<K: Ord> RangeGtEq<K> {
    pub fn new(key: K) -> RangeGtEq<K> {
        RangeGtEq(key)
    }
}

impl<K: Ord> Range for RangeGtEq<K> {
    type Key = K;

    fn compare(&self, key: &Self::Key) -> RangeComparison {
        match key.cmp(&self.0) {
            Ordering::Less => RangeComparison::Less,
            Ordering::Equal | Ordering::Greater => RangeComparison::InRange,
        }
    }
}

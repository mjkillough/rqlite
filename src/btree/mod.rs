mod page;
mod range;

use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

use self::page::*;
pub use self::page::{Cell, InteriorCell};
pub use self::range::*;
use crate::pager::Pager;
use crate::Result;

// Interior pages have an extra right-pointer.
const PAGE_INTERIOR_HEADER_LEN: usize = 12;
const PAGE_LEAF_HEADER_LEN: usize = 8;

pub struct BTree<K, I, L>
where
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
{
    pager: Rc<Pager>,
    page_num: usize,
    phantom: PhantomData<(I, L)>,
}

impl<K, I, L> BTree<K, I, L>
where
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
{
    pub fn new(pager: Rc<Pager>, page_num: usize) -> Result<BTree<K, I, L>> {
        Ok(BTree {
            page_num,
            pager,
            phantom: PhantomData,
        })
    }

    pub fn iter_range<R>(self, range: R) -> BTreeIter<K, I, L, R>
    where
        R: Range<Key = K>,
    {
        let mut iter = BTreeIter {
            pager: self.pager.clone(),
            interiors: vec![],
            leaf: None,
            range,
            last_comparison: RangeComparison::InRange,
        };
        iter.descend(self.page_num);
        iter
    }

    pub fn iter(self) -> BTreeIter<K, I, L, RangeAll<K>> {
        self.iter_range(RangeAll::new())
    }
}

impl<K, I, L> BTree<K, I, L>
where
    K: Ord,
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
{
    pub fn get(self, key: K) -> Option<L> {
        let mut iter: Vec<_> = self.iter_range(RangeOne::new(key)).collect();
        assert!(iter.len() <= 1);
        iter.pop()
    }
}

pub struct BTreeIter<K, I, L, R>
where
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
    R: Range<Key = K>,
{
    pager: Rc<Pager>,
    interiors: Vec<Option<PageIter<I>>>,
    leaf: Option<PageIter<L>>,
    // We remember the last comparison we did, so that when we're ascending
    // back up our stack we can decide whether to visit right-pointers.
    last_comparison: RangeComparison,
    range: R,
}

impl<K, I, L, R> BTreeIter<K, I, L, R>
where
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
    R: Range<Key = K>,
{
    fn descend(&mut self, page_num: usize) {
        let bytes = self.pager.get_page(page_num).unwrap();
        let header_offset = if page_num == 1 { 100 } else { 0 };
        let ty = get_page_type(&bytes, header_offset);
        match ty {
            PageType::Interior => self.interiors.push(Some(
                Page::<I>::new(bytes, header_offset, PAGE_INTERIOR_HEADER_LEN)
                    .unwrap()
                    .iter(),
            )),
            PageType::Leaf => {
                self.leaf = Some(
                    Page::<L>::new(bytes, header_offset, PAGE_LEAF_HEADER_LEN)
                        .unwrap()
                        .iter(),
                )
            }
        };
    }

    fn compare<C: Cell<Key = K>>(&mut self, cell: &C) -> &RangeComparison {
        self.last_comparison = self.range.compare(cell.key());
        &self.last_comparison
    }
}

impl<K, I, L, R> Iterator for BTreeIter<K, I, L, R>
where
    I: InteriorCell<Key = K>,
    L: Cell<Key = K>,
    R: Range<Key = K>,
{
    type Item = L;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match mem::replace(&mut self.leaf, None) {
                // We're iterating through the cells in a leaf page.
                // Attempt to get the next cell and then decide whether to yield it.
                Some(mut leaf) => {
                    match leaf.next() {
                        Some(cell) => {
                            match *self.compare(&cell) {
                                // Silently ignore this value, but continue to  iterate through
                                // the leaf.
                                RangeComparison::Less => {
                                    self.leaf = Some(leaf);
                                }
                                // Return this cell and then continue to iterate through this leaf.
                                RangeComparison::InRange => {
                                    self.leaf = Some(leaf);
                                    return Some(cell);
                                }
                                // All cells that come after this are going to  be Greater. Don't
                                // put self.leaf back, so that we start to ascend back up.
                                _ => {}
                            }
                        }
                        // We've exhausted this leaf. Loop back round and move
                        // one left up our interiors stack.
                        None => {}
                    }
                }
                // We've just finished iterating through the cells in a leaf and
                // now need to move onto the next leaf.
                None => {
                    match self.interiors.pop() {
                        // We were previously iterating through the left-pointer
                        // of one of the cells in this interior page. See if
                        // there's another cell to descend into, otherwise
                        // look at the right-pointer.
                        Some(Some(mut interior)) => {
                            match interior.next() {
                                // There's another cell in this interior page
                                // for us to descend into.
                                Some(cell) => {
                                    self.interiors.push(Some(interior));

                                    match *self.compare(&cell) {
                                        // If the key is Greater than the range, then there's no
                                        // need to descend into the right-pointer, as all keys are
                                        // <= the key. Continue to iterate through this interior
                                        // page, as it may contain bigger keys.
                                        RangeComparison::Greater => {}
                                        _ => {
                                            self.descend(cell.left());
                                        }
                                    }
                                }
                                // There are no more left-pointers on this page.
                                None => {
                                    match self.last_comparison {
                                        // If the last comparison was Greater than the range, or on
                                        // the upper boundary, then we know the right-pointer
                                        // contains only keys which are Greater. Don't descend.
                                        RangeComparison::UpperBoundary
                                        | RangeComparison::Greater => {}
                                        _ => {
                                            // We push None so that we can keep track of our  level
                                            // within the tree. We'll silently move past it when we
                                            // ascend later.
                                            self.interiors.push(None);
                                            self.descend(interior.right());
                                        }
                                    }
                                }
                            }
                        }
                        // We were previously iterating through the right pointer of an
                        // interior page. Ignore it - we'll loop back round and move up
                        // two levels of the stack in one go.
                        Some(None) => {}
                        // Empty interiors stack means we've reached the root again and
                        // have iterated down all of its children (left and right).
                        // We're done!
                        None => return None,
                    }
                }
            }
        }
    }
}

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;

use crate::errors::*;
use crate::pager::Pager;

// Interior pages have an extra right-pointer.
const PAGE_INTERIOR_HEADER_LEN: usize = 12;
const PAGE_LEAF_HEADER_LEN: usize = 8;

pub trait Cell: Sized {
    type Key;

    fn from_bytes(_: Bytes) -> Result<Self>;
    fn key(&self) -> &Self::Key;
}

pub trait InteriorCell: Cell {
    fn left(&self) -> usize;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PageType {
    Interior,
    Leaf,
}

#[derive(Clone, Debug)]
struct Page<C: Cell> {
    data: Bytes,
    header_offset: usize,
    // It would be nice to have a `header_length()` field that was impl
    // for specializations of Page<Leaf>/Page<InteriorCell>, so that
    // we would statically know the header length depending on the type of C.
    header_length: usize,
    phantom: PhantomData<C>,
}

impl<C: Cell> Page<C> {
    pub fn new(data: Bytes, header_offset: usize, header_length: usize) -> Result<Page<C>> {
        Ok(Page {
            data,
            header_offset,
            header_length,
            phantom: PhantomData,
        })
    }

    fn header(&self) -> &[u8] {
        &self.data[self.header_offset..self.header_offset + self.header_length]
    }

    // "The two-byte integer at offset 1 gives the start of the first freeblock
    //  on the page, or is zero if there are no freeblocks."
    fn first_freeblock_offset(&self) -> Option<u16> {
        let offset = BigEndian::read_u16(&self.header()[1..3]);
        match offset {
            0 => None,
            o => Some(o),
        }
    }

    // "The two-byte integer at offset 3 gives the number of cells on the page."
    pub fn len(&self) -> usize {
        BigEndian::read_u16(&self.header()[3..5]) as usize
    }

    // "The two-byte integer at offset 5 designates the start of the cell
    //  content area. A zero value for this integer is interpreted as 65536."
    fn cell_content_offset(&self) -> usize {
        let start = BigEndian::read_u16(&self.header()[5..7]);
        if start == 0 {
            65536
        } else {
            start as usize
        }
    }

    // "The one-byte integer at offset 7 gives the number of fragmented free
    //  bytes within the cell content area."
    fn fragmented_free_bytes(&self) -> u8 {
        self.data[7]
    }

    fn cell_pointers(&self) -> &[u8] {
        let offset = self.header_offset + self.header_length;
        let len = self.len() * 2;
        &self.data[offset..offset + len]
    }

    pub fn cell(&self, index: usize) -> Bytes {
        if index > self.len() {
            panic!("Attempted to access out-of-bounds cell: {}", index);
        }

        let cell_pointer = &self.cell_pointers()[index * 2..];
        let cell_offset = BigEndian::read_u16(cell_pointer) as usize;
        self.data.slice_from(cell_offset)
    }

    pub fn iter(self) -> PageIter<C> {
        PageIter { page: self, idx: 0 }
    }
}

struct PageIter<C: Cell> {
    page: Page<C>,
    idx: usize,
}

impl<I: InteriorCell> PageIter<I> {
    // "The four-byte page number at offset 8 is the right-most pointer. This
    //  value appears in the header of interior b-tree pages only and is omitted
    //  from all other pages."
    fn right(&self) -> usize {
        BigEndian::read_u32(&self.page.header()[8..12]) as usize
    }
}

impl<C: Cell> Iterator for PageIter<C> {
    type Item = C;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.page.len() {
            None
        } else {
            let v = C::from_bytes(self.page.cell(self.idx)).unwrap();
            self.idx += 1;
            Some(v)
        }
    }
}

fn get_page_type(bytes: &Bytes, header_offset: usize) -> PageType {
    if bytes[header_offset] & 0x8 == 0x8 {
        PageType::Leaf
    } else {
        PageType::Interior
    }
}

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
    fn new() -> RangeAll<K> {
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
    fn new(key: K) -> RangeOne<K> {
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
    fn new(key: K) -> RangeGtEq<K> {
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

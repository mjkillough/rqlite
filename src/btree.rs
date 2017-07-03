use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};

use errors::*;
use pager::Pager;


// Interior pages have an extra right-pointer.
const PAGE_INTERIOR_HEADER_LEN: usize = 12;
const PAGE_LEAF_HEADER_LEN: usize = 8;


pub trait Cell: Sized {
    type Key; // TODO: PartialOrd?

    fn from_bytes(Bytes) -> Result<Self>;
    fn key(&self) -> &Self::Key;
}


pub trait InteriorCell: Cell {
    fn left(&self) -> usize;
}


#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PageType {
    // IndexInterior,
    TableInterior,
    // IndexLeaf,
    TableLeaf,
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
        if start == 0 { 65536 } else { start as usize }
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
    match bytes[header_offset] {
        // 0x01 => PageType::IndexInterior,
        0x05 => PageType::TableInterior,
        // 0x0A => PageType::IndexLeaf,
        0x0D => PageType::TableLeaf,
        _ => panic!("Unknown B-Tree page type"),
    }
}


// TODO: Consider getting rid of BTree and having just BTreeIter?
pub struct BTree<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    pager: Rc<Pager>,
    page_num: usize,
    phantom: PhantomData<(I, L)>,
}

impl<I, L> BTree<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    pub fn new(pager: Rc<Pager>, page_num: usize) -> Result<BTree<I, L>> {
        Ok(BTree {
            page_num,
            pager,
            phantom: PhantomData,
        })
    }

    pub fn iter(self) -> BTreeIter<I, L> {
        let mut iter = BTreeIter {
            pager: self.pager.clone(),
            interiors: vec![],
            leaf: None,
        };
        iter.descend(self.page_num);
        iter
    }
}


pub enum Comparison {
    Left,
    Right,
    Both,
}


pub struct BTreeIter<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    pager: Rc<Pager>,
    interiors: Vec<Option<PageIter<I>>>,
    leaf: Option<PageIter<L>>,
}

impl<I, L> BTreeIter<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    fn descend(&mut self, page_num: usize) {
        let bytes = self.pager.get_page(page_num).unwrap();
        let header_offset = if page_num == 1 { 100 } else { 0 };
        let ty = get_page_type(&bytes, header_offset);
        match ty {
            PageType::TableInterior => {
                self.interiors.push(Some(
                    Page::<I>::new(bytes, header_offset, PAGE_INTERIOR_HEADER_LEN)
                        .unwrap()
                        .iter(),
                ))
            }
            PageType::TableLeaf => {
                self.leaf = Some(
                    Page::<L>::new(bytes, header_offset, PAGE_LEAF_HEADER_LEN)
                        .unwrap()
                        .iter(),
                )
            }
        };
    }
}

impl<I, L> Iterator for BTreeIter<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    type Item = L;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match mem::replace(&mut self.leaf, None) {
                Some(mut leaf) => {
                    match leaf.next() {
                        Some(l) => {
                            // Keep iterating through the leaf until it's exhausted.
                            self.leaf = Some(leaf);
                            return Some(l);
                        }
                        // We've exhausted this leaf. Loop back round and move
                        // one left up our interiors stack.
                        None => {}
                    }
                }
                None => {
                    match self.interiors.pop() {
                        Some(Some(mut interior)) => {
                            match interior.next() {
                                Some(cell) => {
                                    // We're about to iterate down one level from iterator,
                                    // so put it back on our stack of interior pages.
                                    self.interiors.push(Some(interior));
                                    self.descend(cell.left());
                                }
                                // There are no more left pointers on this page. We'll iterate down
                                // one level once more into the page's right pointer. We don't push
                                // this interior page back onto the stack (it's done), but we push
                                // None instead to indicate our level in the tree.
                                None => {
                                    self.interiors.push(None);
                                    self.descend(interior.right());
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

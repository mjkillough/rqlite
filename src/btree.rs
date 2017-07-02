use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};

use errors::*;
use pager::Pager;


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
    page_type: PageType,
    data: Bytes,
    header_offset: usize,
    phantom: PhantomData<C>,
}

impl<C: Cell> Page<C> {
    pub fn new(data: Bytes, header_offset: usize) -> Result<Page<C>> {
        let page_type = match data[header_offset] {
            // 0x01 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            // 0x0A => PageType::IndexLeaf,
            0x0D => PageType::TableLeaf,
            _ => Err("Unknown B-Tree page type")?,
        };

        Ok(Page {
            page_type,
            data,
            header_offset,
            phantom: PhantomData,
        })
    }

    fn header_length(&self) -> usize {
        match self.page_type() {
            // PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            // PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        }
    }

    fn header(&self) -> &[u8] {
        &self.data[self.header_offset..self.header_offset + self.header_length()]
    }

    pub fn page_type(&self) -> PageType {
        self.page_type
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

    // "The four-byte page number at offset 8 is the right-most pointer. This
    //  value appears in the header of interior b-tree pages only and is omitted
    //  from all other pages."
    fn right(&self) -> usize {
        if self.header_length() != 12 {
            unreachable!();
        }
        BigEndian::read_u32(&self.header()[8..12]) as usize
    }

    fn cell_pointers(&self) -> &[u8] {
        let offset = self.header_offset + self.header_length();
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
        let bytes = self.pager.get_page(self.page_num).unwrap();
        let header_offset = if self.page_num == 1 { 100 } else { 0 };

        let page_type = match bytes[header_offset] {
            // 0x01 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            // 0x0A => PageType::IndexLeaf,
            0x0D => PageType::TableLeaf,
            _ => panic!("Unknown B-Tree page type"),
        };

        match page_type {
            PageType::TableInterior => {
                let page = Page::<I>::new(bytes, header_offset).unwrap();
                let right = page.right();
                BTreeIter::Interior {
                    pager: self.pager,
                    pages: page.iter(),
                    right: Some(right),
                    inner: None,
                    phantom: PhantomData,
                }
            }
            PageType::TableLeaf => {
                BTreeIter::Leaf::<I, L> {
                    iter: Page::<L>::new(bytes, header_offset).unwrap().iter(),
                    phantom: PhantomData,
                }
            }
        }
    }
}


pub enum BTreeIter<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    Leaf {
        iter: PageIter<L>,
        phantom: PhantomData<(I, L)>,
    },
    Interior {
        pager: Rc<Pager>,
        pages: PageIter<I>,
        right: Option<usize>,
        inner: Option<Box<BTreeIter<I, L>>>,
        phantom: PhantomData<(I, L)>,
    },
}

impl<I, L> Iterator for BTreeIter<I, L>
where
    I: InteriorCell,
    L: Cell,
{
    type Item = L;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            BTreeIter::Leaf { ref mut iter, .. } => iter.next(),
            BTreeIter::Interior {
                ref pager,
                ref mut pages,
                ref mut right,
                ref mut inner,
                ..
            } => {
                loop {
                    match mem::replace(inner, None) {
                        Some(mut iter) => {
                            match iter.next() {
                                None => {
                                    *inner = None;
                                }
                                o => {
                                    *inner = Some(iter);
                                    return o;
                                }
                            }
                        }
                        None => {
                            let page_num = match pages.next() {
                                Some(cell) => cell.left(),
                                None => {
                                    if let Some(page_num) = mem::replace(right, None) {
                                        page_num
                                    } else {
                                        return None;
                                    }
                                }
                            };
                            *inner = Some(Box::new(
                                BTree::new(pager.clone(), page_num).unwrap().iter(),
                            ))
                        }
                    }
                }
            }
        }
    }
}

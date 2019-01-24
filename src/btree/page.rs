use std::marker::PhantomData;

use bytes::{BigEndian, ByteOrder, Bytes};

use crate::Result;

pub trait Cell: Sized {
    type Key;

    fn from_bytes(_: Bytes) -> Result<Self>;
    fn key(&self) -> &Self::Key;
}

pub trait InteriorCell: Cell {
    fn left(&self) -> usize;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PageType {
    Interior,
    Leaf,
}

pub fn get_page_type(bytes: &Bytes, header_offset: usize) -> PageType {
    if bytes[header_offset] & 0x8 == 0x8 {
        PageType::Leaf
    } else {
        PageType::Interior
    }
}

#[derive(Clone, Debug)]
pub struct Page<C: Cell> {
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

    pub fn header(&self) -> &[u8] {
        &self.data[self.header_offset..self.header_offset + self.header_length]
    }

    // "The two-byte integer at offset 1 gives the start of the first freeblock
    //  on the page, or is zero if there are no freeblocks."
    pub fn first_freeblock_offset(&self) -> Option<u16> {
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
    pub fn cell_content_offset(&self) -> usize {
        let start = BigEndian::read_u16(&self.header()[5..7]);
        if start == 0 {
            65536
        } else {
            start as usize
        }
    }

    // "The one-byte integer at offset 7 gives the number of fragmented free
    //  bytes within the cell content area."
    pub fn fragmented_free_bytes(&self) -> u8 {
        self.data[7]
    }

    pub fn cell_pointers(&self) -> &[u8] {
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

pub struct PageIter<C: Cell> {
    page: Page<C>,
    idx: usize,
}

impl<I: InteriorCell> PageIter<I> {
    // "The four-byte page number at offset 8 is the right-most pointer. This
    //  value appears in the header of interior b-tree pages only and is omitted
    //  from all other pages."
    pub fn right(&self) -> usize {
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

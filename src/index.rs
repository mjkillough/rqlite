use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::io::Cursor;
use std::rc::Rc;
use std::result;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};

use btree::{Cell, InteriorCell, BTree, Range, RangeComparison};
use errors::*;
use pager::Pager;
use util::read_varint;
use record::Record;


#[derive(Debug)]
struct IndexLeafCell {
    record: Record,
}

impl Cell for IndexLeafCell {
    type Key = Record;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        // XXX See questions about len in IndexInteriorCell.
        let len = read_varint(&mut cursor)? as usize;
        let position = cursor.position() as usize;
        let bytes = cursor.into_inner().slice(position, position + len);
        let record = Record::from_bytes(bytes)?;

        Ok(IndexLeafCell { record })
    }

    fn key(&self) -> &Self::Key {
        &self.record
    }
}


#[derive(Debug)]
struct IndexInteriorCell {
    left: usize,
    record: Record,
}

impl Cell for IndexInteriorCell {
    type Key = Record;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let left = BigEndian::read_u32(&bytes) as usize;
        let mut cursor = Cursor::new(bytes);
        cursor.set_position(4);
        // XXX Why does sqlite3 bother with len? The length in the record header
        //     should be sufficient. Suggests we're missing something! Maybe
        //     something to do with the overflow which we're ignoring.
        let len = read_varint(&mut cursor)? as usize;
        let position = cursor.position() as usize;
        let bytes = cursor.into_inner().slice(position, position + len);
        let record = Record::from_bytes(bytes)?;

        Ok(IndexInteriorCell { left, record })
    }

    fn key(&self) -> &Self::Key {
        &self.record
    }
}

impl InteriorCell for IndexInteriorCell {
    fn left(&self) -> usize {
        self.left
    }
}


struct IndexRange(Record);

impl IndexRange {
    fn new(record: Record) -> IndexRange {
        IndexRange(record)
    }
}

impl Range for IndexRange {
    type Key = Record;

    fn compare(&self, other: &Self::Key) -> RangeComparison {
        if self.0.len() > other.len() {
            panic!(
                "Attempted to compare records with mis-matched sizes: {:?} {:?}",
                self.0,
                other
            );
        }
        for (this, that) in self.0.iter().zip(other.iter()) {
            let ord = that.partial_cmp(this).unwrap();
            match ord {
                // If Equal, move onto comparing next field.
                Ordering::Equal => {}
                Ordering::Less => return RangeComparison::Less,
                Ordering::Greater => return RangeComparison::Greater,
            }
        }
        // If we got this far, it must be equal.
        RangeComparison::InRange
    }
}


type IndexBTree = BTree<Record, IndexInteriorCell, IndexLeafCell>;


pub struct Index {
    pager: Rc<Pager>,
    page_num: usize,
    tbl_name: String,
    name: String,
}

impl Index {
    pub fn new<S: Into<String>>(
        pager: Rc<Pager>,
        page_num: usize,
        tbl_name: S,
        name: S,
    ) -> Result<Index> {
        let tbl_name = tbl_name.into();
        let name = name.into();
        Ok(Index {
            pager,
            page_num,
            tbl_name,
            name,
        })
    }

    pub fn dump(&self) -> Result<Vec<Record>> {
        let btree = IndexBTree::new(self.pager.clone(), self.page_num)?;
        Ok(btree.iter().map(|cell| cell.record).collect())
    }

    pub fn scan(&self, record: Record) -> Result<Vec<Record>> {
        let btree = IndexBTree::new(self.pager.clone(), self.page_num)?;
        Ok(
            btree
                .iter_range(IndexRange::new(record))
                .map(|cell| cell.record)
                .collect(),
        )
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(
            f,
            "Index for {:?} {{ name: {:?}, page_num: {:?} }}",
            self.tbl_name,
            self.name,
            self.page_num,
        )
    }
}

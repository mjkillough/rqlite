use std::fmt;
use std::io::Cursor;
use std::rc::Rc;
use std::result;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};

use btree::{Cell, InteriorCell, BTree};
use errors::*;
use pager::Pager;
use util::read_varint;
use record::{parse_record, Field};


type IndexKey = Vec<Field>;


#[derive(Debug)]
struct IndexLeafCell {
    fields: IndexKey,
}

impl Cell for IndexLeafCell {
    type Key = IndexKey;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        // XXX See questions about len in IndexInteriorCell.
        let len = read_varint(&mut cursor)? as usize;
        let position = cursor.position() as usize;
        let fields = parse_record(cursor.into_inner().slice(position, position + len))?;

        Ok(IndexLeafCell { fields })
    }

    fn key(&self) -> &Self::Key {
        &self.fields
    }
}


#[derive(Debug)]
struct IndexInteriorCell {
    left: usize,
    fields: IndexKey,
}

impl Cell for IndexInteriorCell {
    type Key = IndexKey;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let left = BigEndian::read_u32(&bytes) as usize;
        let mut cursor = Cursor::new(bytes);
        cursor.set_position(4);
        // XXX Why does sqlite3 bother with len? The length in the record header
        //     should be sufficient. Suggests we're missing something! Maybe
        //     something to do with the overflow which we're ignoring.
        let len = read_varint(&mut cursor)? as usize;
        let position = cursor.position() as usize;
        let fields = parse_record(cursor.into_inner().slice(position, position + len))?;

        Ok(IndexInteriorCell { left, fields })
    }

    fn key(&self) -> &Self::Key {
        &self.fields
    }
}

impl InteriorCell for IndexInteriorCell {
    fn left(&self) -> usize {
        self.left
    }
}


type IndexBTree = BTree<IndexKey, IndexInteriorCell, IndexLeafCell>;


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

    pub fn dump(&self) -> Result<Vec<Vec<Field>>> {
        let btree = IndexBTree::new(self.pager.clone(), self.page_num)?;
        Ok(btree.iter().map(|cell| cell.fields).collect())
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

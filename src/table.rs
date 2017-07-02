use std::io::Cursor;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};

use btree::{Cell, InteriorCell};
use errors::*;
use util::read_varint;
use record::{parse_record, Field};


pub struct TableLeafCell {
    pub row_id: u64,
    pub fields: Vec<Field>,
}

impl Cell for TableLeafCell {
    type Key = u64;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let payload_length = read_varint(&mut cursor)?;
        let row_id = read_varint(&mut cursor)?;
        let position = cursor.position() as usize;
        let fields = parse_record(cursor.into_inner().slice_from(position))?;

        Ok(TableLeafCell { row_id, fields })
    }

    fn key(&self) -> &Self::Key {
        &self.row_id
    }
}


pub struct TableInteriorCell {
    row_id: u64,
    left: usize,
}

impl Cell for TableInteriorCell {
    type Key = u64;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let left = BigEndian::read_u32(&bytes) as usize;
        let row_id = read_varint(&mut Cursor::new(bytes))?;
        Ok(TableInteriorCell { row_id, left })
    }

    fn key(&self) -> &Self::Key {
        &self.row_id
    }
}

impl InteriorCell for TableInteriorCell {
    fn left(&self) -> usize {
        self.left
    }
}

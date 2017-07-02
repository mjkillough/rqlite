use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::io::Cursor;
use std::rc::Rc;
use std::result;

use bytes::Bytes;
use byteorder::{ByteOrder, BigEndian};
use nom_sql::{self, SqlQuery, SqlType, CreateTableStatement, SelectStatement, FieldExpression};

use btree::{Cell, InteriorCell, BTree};
use errors::*;
use pager::Pager;
use util::read_varint;
use record::{parse_record, Field};
use types::Type;


#[derive(Debug)]
struct Column {
    name: String,
    ty: Type,
}


#[derive(Debug)]
struct TableSchema {
    columns: Vec<Column>,
}

impl TableSchema {
    fn from_sql(sql: &str) -> Result<TableSchema> {
        let parsed = nom_sql::parser::parse_query(sql)?;
        let column_defs = match parsed {
            SqlQuery::CreateTable(CreateTableStatement { fields, .. }) => fields,
            _ => bail!("Expected CREATE TABLE: {}", sql),
        };

        let columns: Result<Vec<_>> = column_defs
            .into_iter()
            .map(|col| {
                let ty = match col.sql_type {
                    SqlType::Int(_) => Type::Integer,
                    SqlType::Text => Type::Text,
                    other => bail!("Unexpected column type: {:?}", other),
                };
                Ok(Column {
                    name: col.column.name,
                    ty,
                })
            })
            .collect();

        Ok(TableSchema { columns: columns? })
    }

    fn column_indices<S: AsRef<str>>(&self, names: &[S]) -> Result<Vec<usize>> {
        names
            .iter()
            .map(|name| {
                self.columns
                    .iter()
                    .position(|col| col.name == name.as_ref())
                    .ok_or(format!("Unknown column: {}", name.as_ref()).into())
            })
            .collect()
    }
}


#[derive(Debug)]
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


#[derive(Debug)]
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


type TableBTree = BTree<TableInteriorCell, TableLeafCell>;


pub struct Table {
    pager: Rc<Pager>,
    page_num: usize,
    name: String,
    schema: TableSchema,
}

impl Table {
    pub fn new<S: Into<String>>(
        pager: Rc<Pager>,
        page_num: usize,
        name: S,
        sql: &str,
    ) -> Result<Table> {
        Ok(Table {
            pager,
            page_num,
            name: name.into(),
            schema: TableSchema::from_sql(sql)?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn select<S: Into<String>>(&self, columns: Vec<S>) -> Result<Vec<HashMap<String, Field>>> {
        let columns: Vec<String> = columns.into_iter().map(|s| s.into()).collect();
        let indices = self.schema.column_indices(&columns)?;

        let btree = TableBTree::new(self.pager.clone(), self.page_num)?;
        let results = btree
            .iter()
            .map(|row| {
                row.fields
                    .into_iter()
                    .enumerate()
                    .filter(|&(i, _)| indices.contains(&i))
                    .zip(columns.clone())
                    .map(|((i, field), name)| (name, field))
                    .collect()
            })
            .collect();

        Ok(results)
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(
            f,
            "Table {{ name: {:?}, page_num: {:?}, schema: {:?} }}",
            self.name,
            self.page_num,
            self.schema
        )
    }
}

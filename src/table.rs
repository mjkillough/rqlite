use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::io::Cursor;
use std::rc::Rc;
use std::result;

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use nom_sql::{
    self, ColumnConstraint, CreateTableStatement, FieldExpression, SelectStatement, SqlQuery,
    SqlType,
};

use crate::btree::{BTree, Cell, InteriorCell};
use crate::errors::*;
use crate::pager::Pager;
use crate::record::{Field, Record};
use crate::types::Type;
use crate::util::read_varint;

#[derive(Debug)]
enum ColumnReference {
    RowId,
    Index(usize),
}

#[derive(Debug)]
struct Column {
    name: String,
    ty: Type,
    primary_key: bool,
}

#[derive(Debug)]
struct TableSchema {
    columns: Vec<Column>,
}

impl TableSchema {
    fn from_sql(sql: &str) -> Result<TableSchema> {
        let parsed = nom_sql::parser::parse_query(sql)
            .map_err(|_| format!("Failed to parse schema: {}", sql))?;
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
                let primary_key = col.constraints.contains(&ColumnConstraint::PrimaryKey);
                Ok(Column {
                    name: col.column.name,
                    ty,
                    primary_key,
                })
            })
            .collect();

        Ok(TableSchema { columns: columns? })
    }

    fn column_indices<S: AsRef<str>>(&self, names: &[S]) -> Result<Vec<ColumnReference>> {
        // If the primary key is a single integer column, then it is
        // actually stored as the RowId and a null is stored in its place
        // in the fields.
        let pks = self
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .collect::<Vec<_>>();
        let pk_is_rowid = pks.len() == 1 && pks[0].ty == Type::Integer;
        names
            .iter()
            .map(|name| {
                if pk_is_rowid && pks[0].name == name.as_ref() {
                    Ok(ColumnReference::RowId)
                } else {
                    let idx = self
                        .columns
                        .iter()
                        .position(|col| col.name == name.as_ref())
                        .ok_or(format!("Unknown column: {}", name.as_ref()))?;
                    Ok(ColumnReference::Index(idx))
                }
            })
            .collect()
    }
}

type CellKey = u64;

#[derive(Debug)]
pub struct TableLeafCell {
    pub row_id: u64,
    pub record: Record,
}

impl Cell for TableLeafCell {
    type Key = CellKey;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let _payload_length = read_varint(&mut cursor)?;
        let row_id = read_varint(&mut cursor)?;
        let position = cursor.position() as usize;
        let bytes = cursor.into_inner().slice_from(position);
        let record = Record::from_bytes(bytes)?;

        Ok(TableLeafCell { row_id, record })
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
    type Key = CellKey;

    fn from_bytes(bytes: Bytes) -> Result<Self> {
        // XXX Are we reading the same bytes as left and row_id?
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

type TableBTree = BTree<CellKey, TableInteriorCell, TableLeafCell>;

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

    pub fn len(&self) -> Result<usize> {
        let btree = TableBTree::new(self.pager.clone(), self.page_num)?;
        Ok(btree.iter().collect::<Vec<_>>().len())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn select<S: Into<String>>(&self, columns: Vec<S>) -> Result<Vec<HashMap<String, Field>>> {
        let columns: Vec<String> = columns.into_iter().map(|s| s.into()).collect();
        let colrefs = self.schema.column_indices(&columns)?;

        let btree = TableBTree::new(self.pager.clone(), self.page_num)?;
        let results = btree
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .zip(colrefs.iter())
                    .map(|(name, colref)| {
                        let value = match *colref {
                            ColumnReference::RowId => Field::from(*row.key()),
                            ColumnReference::Index(idx) => row.record[idx].clone(), // XXX rethink
                        };
                        (name.clone(), value)
                    })
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
            self.name, self.page_num, self.schema
        )
    }
}

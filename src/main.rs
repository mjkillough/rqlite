extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate error_chain;
extern crate nom_sql;

mod btree;
mod db;
mod errors;
mod pager;
mod record;
mod schema;
mod table;
mod types;
mod util;

use std::rc::Rc;

use btree::BTree;
use errors::*;
use pager::Pager;
use record::Field;
use schema::Schema;
use table::Table;

use std::fmt::Display;

use nom_sql::{SqlQuery, SqlType, CreateTableStatement, SelectStatement, FieldExpression};


struct SelectOp {
    table: String,
    columns: Vec<String>,
}

impl SelectOp {
    fn from_sql(sql: &str) -> Result<SelectOp> {
        let parsed = nom_sql::parser::parse_query(sql)?;
        let (mut tables, fields) = match parsed {
            SqlQuery::Select(SelectStatement { tables, fields, .. }) => (tables, fields),
            _ => bail!("Expected SELECT: {}", sql),
        };

        if tables.len() != 1 {
            bail!("Expected 1 table to appear in SELECT statement: {}", sql);
        }
        let table = tables.pop().unwrap().name;

        let columns: Result<Vec<_>> = fields
            .into_iter()
            .map(|field| match field {
                FieldExpression::Col(nom_sql::Column { name, .. }) => Ok(name),
                _ => bail!("Not implemented: non-column fields in SELECT: {}", sql),
            })
            .collect();

        Ok(SelectOp {
            table,
            columns: columns?,
        })
    }
}


fn run() -> Result<()> {
    let mut pager = Rc::new(Pager::open("aFile.db")?);
    println!(
        "Page Size: {}, Reserved Bytes Per Page: {}, Num Pages: {}",
        pager.header.page_size,
        pager.header.reserved_byes_per_page,
        pager.header.num_pages
    );

    let schema = Schema::new(pager)?;
    println!("Tables: {:#?}", schema.tables()?);

    Ok(())
}

quick_main!(run);

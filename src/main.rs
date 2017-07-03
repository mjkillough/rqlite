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

use std::fmt::Display;
use std::io::{self, Read, Write};
use std::rc::Rc;

use btree::BTree;
use errors::*;
use pager::Pager;
use record::Field;
use schema::Schema;
use table::Table;

use nom_sql::{SqlQuery, SqlType, CreateTableStatement, SelectStatement, FieldExpression};


#[derive(Debug)]
struct SelectOp {
    table: String,
    columns: Vec<String>,
}

impl SelectOp {
    fn from_stmt(stmt: SelectStatement) -> Result<SelectOp> {
        let (mut tables, fields) = (stmt.tables, stmt.fields);

        if tables.len() != 1 {
            bail!("Expected 1 table to appear in SELECT statement");
        }
        let table = tables.pop().unwrap().name;

        let columns: Result<Vec<_>> = fields
            .into_iter()
            .map(|field| match field {
                FieldExpression::Col(nom_sql::Column { name, .. }) => Ok(name),
                _ => bail!("Not implemented: non-column fields in SELECT"),
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

    loop {
        print!("> ");
        io::stdout().flush();
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;

        if buffer.trim() == ".quit" {
            break;
        }

        if buffer.starts_with(".count ") {
            let (_, table_name) = buffer.split_at(7);
            match schema.table(table_name.trim()) {
                Ok(table) => {
                    match table.len() {
                        Ok(len) => println!("{}", len),
                        Err(e) => println!("Failed to get size of table {}: {}", table_name, e),
                    }
                }
                Err(e) => println!("Unknown table: {}", table_name),
            }
            continue;
        }

        let statement = match nom_sql::parser::parse_query(&buffer) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("Error parsing statement: {}", e);
                continue;
            }
        };

        // XXX Combinators please! (It's late and I'm tired).
        match statement {
            SqlQuery::Select(select) => {
                match SelectOp::from_stmt(select) {
                    Ok(op) => {
                        match schema.table(op.table) {
                            Ok(table) => {
                                match table.select(op.columns) {
                                    Ok(result) => println!("{:?}", result),
                                    Err(e) => {
                                        println!("Error running query: {}", e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Error getting table: {}", e);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error processing statement: {}", e);
                        continue;
                    }
                }
            }
            _ => println!("Unsupported statement - SELECT only please"),
        };


    }

    Ok(())
}

quick_main!(run);

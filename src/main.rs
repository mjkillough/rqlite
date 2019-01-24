

#[macro_use]
extern crate error_chain;
use nom_sql;

mod btree;
mod db;
mod errors;
mod index;
mod pager;
mod record;
mod schema;
mod table;
mod types;
mod util;

use std::fmt::Display;
use std::io::{self, Read, Write};
use std::rc::Rc;

use crate::btree::BTree;
use crate::errors::*;
use crate::pager::Pager;
use crate::record::{Field, Record};
use crate::schema::Schema;
use crate::table::Table;

use nom_sql::{CreateTableStatement, FieldExpression, SelectStatement, SqlQuery, SqlType};

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

fn run_query(schema: &Schema, query: &str) -> Result<()> {
    let stmt = nom_sql::parser::parse_query(&query)
        .map_err(|_| format!("Error parsing statement: {}", query))?;
    match stmt {
        SqlQuery::Select(select) => {
            let op =
                SelectOp::from_stmt(select).chain_err(|| format!("Error processing statement:"))?;
            let table = schema.table(op.table)?;
            let result = table
                .select(op.columns)
                .chain_err(|| format!("Error running query:"));
            println!("{:?}", result)
        }
        _ => bail!("Unsupported statement - SELECT only please"),
    };
    Ok(())
}

fn run() -> Result<()> {
    let pager = Rc::new(Pager::open("aFile.db")?);
    println!(
        "Page Size: {}, Reserved Bytes Per Page: {}, Num Pages: {}",
        pager.header.page_size, pager.header.reserved_byes_per_page, pager.header.num_pages
    );

    let schema = Schema::new(pager)?;
    println!("Tables: {:#?}", schema.tables()?);
    let indices = schema.indices()?;
    for (i, index) in indices.iter().enumerate() {
        println!("{:?}", index);
        for row in index.dump()? {
            println!("  > {:?}", row);
        }
        if i == 1 {
            println!("Scanning:");
            let record = Record::new(vec![Field::from("value1")]);
            for row in index.scan(record)? {
                println!("  > {:?}", row)
            }
        }
    }

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
                Ok(table) => match table.len() {
                    Ok(len) => println!("{}", len),
                    Err(e) => println!("Failed to get size of table {}: {}", table_name, e),
                },
                Err(_e) => println!("Unknown table: {}", table_name),
            }
            continue;
        }

        if let Err(e) = run_query(&schema, &buffer) {
            println!("Error running query: {}", e)
        }
    }

    Ok(())
}

quick_main!(run);

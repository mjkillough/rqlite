use std::collections::HashMap;
use std::rc::Rc;

use crate::errors::*;
use crate::index::Index;
use crate::pager::Pager;
use crate::table::Table;

const SQLITE_MASTER_SCHEMA: &'static str = "
    CREATE TABLE sqlite_master(
        type text,
        name text,
        tbl_name text,
        rootpage int,
        sql text
    );
"; // XXX should be "rootpage integer" but there's a bug in nom-sql

pub struct Schema {
    pager: Rc<Pager>,
    schema_table: Table,
}

impl Schema {
    pub fn new(pager: Rc<Pager>) -> Result<Schema> {
        let schema_table = Table::new(pager.clone(), 1, "sqlite_master", SQLITE_MASTER_SCHEMA)?;

        Ok(Schema {
            pager,
            schema_table,
        })
    }

    pub fn indices(&self) -> Result<Vec<Index>> {
        self.schema_table
            .select(vec!["type", "name", "tbl_name", "rootpage", "sql"])?
            .iter()
            .filter(|row| row["type"].as_text().unwrap_or("") == "index")
            .map(|row| {
                Index::new(
                    self.pager.clone(),
                    row["rootpage"].as_integer()? as usize,
                    row["tbl_name"].as_text()?,
                    row["name"].as_text()?,
                )
            })
            .collect()
    }

    pub fn tables(&self) -> Result<Vec<Table>> {
        self.schema_table
            .select(vec!["type", "tbl_name", "rootpage", "sql"])?
            .iter()
            .filter(|table| table["type"].as_text().unwrap_or("") == "table")
            .map(|table| {
                let page_num = table["rootpage"].as_integer()? as usize;
                let name = table["tbl_name"].as_text()?;
                let sql = table["sql"].as_text()?;
                Table::new(self.pager.clone(), page_num, name, &sql)
            })
            .collect()
    }

    pub fn table<S: AsRef<str>>(&self, name: S) -> Result<Table> {
        // XXX we should defs re-use that iterator up there
        self.tables()?
            .into_iter()
            .find(|t| t.name() == name.as_ref())
            .ok_or(ErrorKind::TableDoesNotExist(name.as_ref().to_owned()).into())
    }
}

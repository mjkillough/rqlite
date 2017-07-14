use std::collections::HashMap;
use std::rc::Rc;

use errors::*;
use index::Index;
use pager::Pager;
use table::Table;


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
            .filter(|row| {
                row["type"].text().unwrap_or(&[]) == "index".as_bytes()
            })
            .map(|row| {
                Index::new(
                    self.pager.clone(),
                    row["rootpage"].integer()? as usize,
                    // XXX Assuming UTF-8
                    String::from_utf8(row["tbl_name"].text()?.to_vec())?,
                    String::from_utf8(row["name"].text()?.to_vec())?,
                )
            })
            .collect()
    }

    pub fn tables(&self) -> Result<Vec<Table>> {
        self.schema_table
            .select(vec!["type", "tbl_name", "rootpage", "sql"])?
            .iter()
            .filter(|table| {
                table["type"].text().unwrap_or(&[]) == "table".as_bytes()
            })
            .map(|table| {
                let page_num = table["rootpage"].integer()? as usize;
                // XXX Not necessarily UTF-8!
                let name = String::from_utf8(table["tbl_name"].text()?.to_vec())?;
                let sql = String::from_utf8(table["sql"].text()?.to_vec())?;
                Table::new(self.pager.clone(), page_num, name, &sql)
            })
            .collect()
    }

    pub fn table<S: AsRef<str>>(&self, name: S) -> Result<Table> {
        // XXX we should defs re-use that iterator up there
        self.tables()?
            .into_iter()
            .find(|t| t.name() == name.as_ref())
            .ok_or(
                ErrorKind::TableDoesNotExist(name.as_ref().to_owned()).into(),
            )
    }
}

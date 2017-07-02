use std::rc::Rc;

use errors::*;
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

    pub fn tables(&self) -> Result<Vec<Table>> {
        let v: Result<Vec<Table>> = self.schema_table
            .select(vec!["type", "tbl_name", "rootpage", "sql"])?
            .iter()
            .filter(|table| {
                table["type"].value().text().unwrap_or(&[]) == "table".as_bytes()
            })
            .map(|table| {
                let page_num = table["rootpage"].value().integer()? as usize;
                // XXX Not necessarily UTF-8!
                let name = String::from_utf8(table["tbl_name"].value().text()?.to_vec())?;
                let sql = String::from_utf8(table["sql"].value().text()?.to_vec())?;
                Table::new(self.pager.clone(), page_num, name, &sql)
            })
            .collect();
        Ok(v?)
    }
}

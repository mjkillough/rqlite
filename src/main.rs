extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate error_chain;

mod btree;
mod db;
mod errors;
mod pager;
mod record;
mod table;
mod util;

use std::rc::Rc;

use btree::BTree;
use errors::*;
use pager::Pager;
use table::{TableInteriorCell, TableLeafCell};


fn dump_table_cell(cell: TableLeafCell) -> Result<()> {
    println!(
        "Data: {:?}",
        cell.fields
            .iter()
            .map(|f| f.value().to_string())
            .collect::<Vec<_>>()
    );

    Ok(())
}


fn run() -> Result<()> {
    let mut pager = Rc::new(Pager::open("aFile.db")?);
    println!(
        "Page Size: {}, Reserved Bytes Per Page: {}, Num Pages: {}",
        pager.header.page_size,
        pager.header.reserved_byes_per_page,
        pager.header.num_pages
    );


    let btree = BTree::<TableInteriorCell, TableLeafCell>::new(pager, 3)?;
    for cell in btree.iter() {
        dump_table_cell(cell);
    }

    Ok(())
}

quick_main!(run);

use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;

use bytes::Bytes;

use crate::db::DbHeader;
use crate::errors::*;

pub struct Pager {
    file: RefCell<File>,
    pub header: DbHeader,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Pager> {
        let mut file = File::open(path)?;

        let mut buffer = [0; 100];
        file.read_exact(&mut buffer)
            .chain_err(|| ErrorKind::InvalidDbHeader("Error reading header".to_owned()))?;
        let header = DbHeader::parse(&buffer)?;

        Ok(Pager {
            file: RefCell::new(file),
            header,
        })
    }

    pub fn get_page(&self, number: usize) -> Result<Bytes> {
        // SQLite counts pages from 1.
        let number = number - 1;

        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start((number * self.header.page_size) as u64))?;
        let mut buffer = vec![0; self.header.page_size];
        file.read_exact(&mut buffer)?;
        Ok(buffer.into())
    }
}

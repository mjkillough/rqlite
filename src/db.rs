use byteorder::{BigEndian, ByteOrder};

use crate::errors::*;

pub struct DbHeader {
    pub page_size: usize,
    pub reserved_byes_per_page: usize,
    pub num_pages: usize,
}

impl DbHeader {
    pub fn parse(data: &[u8]) -> Result<DbHeader> {
        use crate::ErrorKind::InvalidDbHeader;

        const s: &'static str = "SQLite format 3\0";
        if data.len() < s.len() || &data[..s.len()] != s.as_bytes() {
            bail!(InvalidDbHeader(format!(
                "Invalid header string: {:?}",
                &data[..s.len()]
            ),));
        }

        // "The database page size in bytes. Must be a power of two between 512
        //  and 32768 inclusive, or the value 1 representing a page size of 65536."
        let page_size = match BigEndian::read_u16(&data[16..]) {
            1 => 65536,
            n if n >= 512 && n <= 32768 && (n & (n - 1)) == 0 => n as usize,
            n => bail!(InvalidDbHeader(format!("Invalid page size: {}", n))),
        };

        Ok(DbHeader {
            page_size,
            // "Bytes of unused "reserved" space at the end of each page. Usually 0."
            reserved_byes_per_page: data[20] as usize,
            // "Size of the database file in pages. The "in-header database size"."
            num_pages: BigEndian::read_u32(&data[28..]) as usize,
        })
    }
}

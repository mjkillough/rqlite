extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate error_chain;

mod errors;
mod record;

use std::fs::File;
use std::io::{Cursor, SeekFrom};
use std::io::prelude::*;
use std::iter::Iterator;
use std::ops::Index;
use std::path::Path;
use std::string::ToString;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};

use errors::*;
use record::parse_record;


/// Read's a 64-bit variable length integer.
///
/// From sqlite3's btreeInt.h:
///
/// > Cell content makes use of variable length integers.  A variable
/// > length integer is 1 to 9 bytes where the lower 7 bits of each
/// > byte are used.  The integer consists of all bytes that have bit 8 set and
/// > the first byte with bit 8 clear.  The most significant byte of the integer
/// > appears first.  A variable-length integer may not be more than 9 bytes long.
/// > As a special case, all 8 bytes of the 9th byte are used as data.  This
/// > allows a 64-bit integer to be encoded in 9 bytes.
///
/// This function returns "InvalidVarint" if less than 9 bytes long and the final
/// byte has bit 8 set.
fn read_varint<R: Read>(mut data: R) -> Result<u64> {
    // sqlite3's implementation in util.c seems to have manually unrolled the
    // loop, so that the small number of bytes cases are optimised. Once we're
    // working, we should profile to see whether any optimisation is needed
    // - although I expect the compiler is able to do it.

    // If we're using the 9th byte then we need to treat it specially, as we
    // use all 8 bits from it.
    let mut value: u64 = 0;
    let mut seen_last_byte = false;
    for i in 0..8 {
        let mut buffer = [0];
        data.read_exact(&mut buffer)
            .chain_err(|| ErrorKind::InvalidVarint)?;
        value = (value << 7) | ((buffer[0] & 0x7F) as u64);
        if buffer[0] & 0x80 == 0 {
            seen_last_byte = true;
            break;
        }
    }
    if !seen_last_byte {
        let mut buffer = [0];
        data.read_exact(&mut buffer)
            .chain_err(|| ErrorKind::InvalidVarint)?;
        value = (value << 8) | (buffer[0] as u64);
    }

    Ok(value)
}


#[cfg(test)]
mod test {
    use super::read_varint;

    use std::io::Cursor;

    #[test]
    fn test_read_varint() {
        // From examples in sqlite3's btreeInt.h:
        assert_eq!(read_varint(Cursor::new(&[0x00])).unwrap(), 0x00000000);
        assert_eq!(read_varint(Cursor::new(&[0x7f])).unwrap(), 0x0000007f);
        assert_eq!(read_varint(Cursor::new(&[0x81, 0x00])).unwrap(), 0x00000080);
        assert_eq!(read_varint(Cursor::new(&[0x82, 0x00])).unwrap(), 0x00000100);
        assert_eq!(read_varint(Cursor::new(&[0x80, 0x7f])).unwrap(), 0x0000007f);
        // ???
        // assert_eq!(read_varint(Cursor::new(&[0x8a, 0x91, 0xd1, 0xac,) 0x78], 0x1234567);
        assert_eq!(
            read_varint(Cursor::new(&[0x81, 0x81, 0x81, 0x81, 0x01])).unwrap(),
            0x10204081
        );

        // TODO: Test we use all 8 bits from the 9th byte.
    }

    #[test]
    fn test_read_varint_fails_with_small_buffer() {
        // The buffer is less than 9 bytes long and the final byte doesn't
        // have it's most significant bit clear.
        assert!(read_varint(Cursor::new(&[0x81])).is_err());
    }
}


#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PageType {
    // IndexInterior,
    TableInterior,
    // IndexLeaf,
    TableLeaf,
}


struct Page {
    page_type: PageType,
    data: Bytes,
    header_offset: usize,
}

impl Page {
    pub fn new(data: Bytes, header_offset: usize) -> Result<Page> {
        let page_type = match data[header_offset] {
            // 0x01 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            // 0x0A => PageType::IndexLeaf,
            0x0D => PageType::TableLeaf,
            _ => Err("Unknown B-Tree page type")?,
        };

        Ok(Page {
            page_type,
            data,
            header_offset,
        })
    }

    fn header_length(&self) -> usize {
        match self.page_type() {
            // PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            // PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        }
    }

    fn header(&self) -> &[u8] {
        &self.data[self.header_offset..self.header_offset + self.header_length()]
    }

    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    // "The two-byte integer at offset 1 gives the start of the first freeblock
    //  on the page, or is zero if there are no freeblocks."
    fn first_freeblock_offset(&self) -> Option<u16> {
        let offset = BigEndian::read_u16(&self.header()[1..3]);
        match offset {
            0 => None,
            o => Some(o),
        }
    }

    // "The two-byte integer at offset 3 gives the number of cells on the page."
    fn num_cells(&self) -> usize {
        BigEndian::read_u16(&self.header()[3..5]) as usize
    }

    // "The two-byte integer at offset 5 designates the start of the cell
    //  content area. A zero value for this integer is interpreted as 65536."
    fn cell_content_offset(&self) -> usize {
        let start = BigEndian::read_u16(&self.header()[5..7]);
        if start == 0 { 65536 } else { start as usize }
    }

    // "The one-byte integer at offset 7 gives the number of fragmented free
    //  bytes within the cell content area."
    fn fragmented_free_bytes(&self) -> u8 {
        self.data[7]
    }

    // "The four-byte page number at offset 8 is the right-most pointer. This
    //  value appears in the header of interior b-tree pages only and is omitted
    //  from all other pages."
    fn rightmost_pointer(&self) -> u32 {
        if self.header_length() != 12 {
            unreachable!();
        }
        BigEndian::read_u32(&self.header()[8..12])
    }

    fn cell_contents(&self) -> &[u8] {
        &self.data[self.cell_content_offset()..]
    }

    fn cells(&self) -> Cells {
        Cells { page: self }
    }
}


struct Cells<'a> {
    page: &'a Page,
}

impl<'a> Cells<'a> {
    pub fn len(&self) -> usize {
        self.page.num_cells()
    }

    pub fn iter(&self) -> CellsIter {
        CellsIter {
            cells: self,
            idx: 0,
        }
    }

    fn cell_pointers(&self) -> &[u8] {
        let offset = self.page.header_offset + self.page.header_length();
        let len = self.page.num_cells() * 2;
        &self.page.data[offset..offset + len]
    }
}

impl<'a> Index<usize> for Cells<'a> {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        if index > self.page.num_cells() {
            panic!("Attempted to access out-of-bounds cell: {}", index);
        }

        let cell_pointer = self.cell_pointers()[index * 2..];
        let cell_offset = BigEndian::read_u16(&cell_pointer) as usize;
        &self.page.data[cell_offset..]
    }
}


struct CellsIter<'a> {
    cells: &'a Cells<'a>,
    idx: usize,
}

impl<'a> Iterator for CellsIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.cells.len() {
            None
        } else {
            let v = &self.cells[self.idx];
            self.idx += 1;
            Some(v)
        }
    }
}


struct DbHeader {
    page_size: usize,
    reserved_byes_per_page: usize,
    num_pages: usize,
}

impl DbHeader {
    fn parse(data: &[u8]) -> Result<DbHeader> {
        use ErrorKind::InvalidDbHeader;

        const s: &'static str = "SQLite format 3\0";
        if data.len() < s.len() || &data[..s.len()] != s.as_bytes() {
            bail!(InvalidDbHeader(
                format!("Invalid header string: {:?}", &data[..s.len()]),
            ));
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


struct Pager {
    file: File,
    header: DbHeader,
}

impl Pager {
    fn open<P: AsRef<Path>>(path: P) -> Result<Pager> {
        let mut file = File::open(path)?;

        let mut buffer = [0; 100];
        file.read_exact(&mut buffer)
            .chain_err(|| {
                ErrorKind::InvalidDbHeader("Error reading header".to_owned())
            })?; // XXX String?
        let header = DbHeader::parse(&buffer)?;

        Ok(Pager { file, header })
    }

    fn get_page(&mut self, number: usize) -> Result<Bytes> {
        // SQLite counts pages from 1.
        let number = number - 1;

        self.file
            .seek(SeekFrom::Start((number * self.header.page_size) as u64));
        let mut buffer = vec![0; self.header.page_size];
        self.file.read_exact(&mut buffer)?;
        Ok(buffer.into())
    }
}


fn dump_cell(buffer: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(buffer);
    let payload_length = read_varint(&mut cursor)?;
    let rowid = read_varint(&mut cursor);
    let position = cursor.position() as usize;
    let fields = parse_record(&cursor.into_inner()[position..])?;
    println!(
        "Data: {:?}",
        fields
            .iter()
            .map(|f| f.value().to_string())
            .collect::<Vec<_>>()
    );

    Ok(())
}


fn run() -> Result<()> {
    let mut pager = Pager::open("aFile.db")?;
    println!(
        "Page Size: {}, Reserved Bytes Per Page: {}, Num Pages: {}",
        pager.header.page_size,
        pager.header.reserved_byes_per_page,
        pager.header.num_pages
    );


    let page = Page::new(pager.get_page(1)?, 100)?;

    // let mut file = File::open("aFile.db")?;
    // let mut contents = Vec::new();
    // file.read_to_end(&mut contents)?;

    // let header = DbHeader::parse(&contents)?;

    // let page_number = 2;
    // let page_start = page_number * header.page_size;
    // let page_end = page_start + header.page_size;
    // let header_offset = if page_number == 0 { 100 } else { 0 };

    // let page = Page::new(&contents[page_start..page_end], header_offset)?;
    // println!("Page type: {:?}", page.page_type());
    // println!("Num cells: {:?}", page.num_cells());
    // println!("Cell content size: {:?}", page.cell_contents().len());
    // println!("Cell content offset: {}", page.cell_content_offset());

    for cell in page.cells().iter() {
        dump_cell(cell);
    }

    // for i in 0..(page.num_cells() as usize) {
    //     let cell_offset = BigEndian::read_u16(
    //         &page.data[page.header_offset + page.header_length() + (i * 2)..],
    //     ) as usize;
    //     dump_cell(&page.data[cell_offset..]);
    // }

    parse_record(&page.cell_contents()[2..]);

    Ok(())
}

quick_main!(run);

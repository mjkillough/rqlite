extern crate byteorder;
#[macro_use]
extern crate error_chain;

mod errors;

use std::fs::File;
use std::io::Cursor;
use std::io::prelude::*;
use std::ops::Index;
use std::string::ToString;

use byteorder::{BigEndian, ByteOrder};

use errors::*;


/// Read's a 64-bit variable length integer from the start of the buffer.
///
/// Returns the integer and the number of bytes read from the buffer.
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
/// This function panics if the buffer is less than 9 bytes long and the final
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
        data.read_exact(&mut buffer)?;
        value = (value << 7) | ((buffer[0] & 0x7F) as u64);
        if buffer[0] & 0x80 == 0 {
            seen_last_byte = true;
            break;
        }
    }
    if !seen_last_byte {
        // We need to include all bits from the final (9th) byte.
        let mut buffer = [0];
        data.read_exact(&mut buffer)?;
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
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}


struct Page<'a> {
    page_type: PageType,
    data: &'a [u8],
    header_offset: usize,
}

impl<'a> Page<'a> {
    pub fn new(data: &'a [u8], header_offset: usize) -> Result<Page<'a>> {
        let page_type = match data[header_offset] {
            0x01 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0A => PageType::IndexLeaf,
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
            IndexInterior => 12,
            TableInterior => 12,
            IndexLeaf => 8,
            TableLeaf => 8,
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
    fn num_cells(&self) -> u16 {
        BigEndian::read_u16(&self.header()[3..5])
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
    // fn cell_pointers(&self) ->
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FieldType {
    Null,
    U8,
    U16,
    U24,
    U32,
    U48,
    U64,
    F64,
    Zero,
    One,
    Blob(usize),
    Str(usize),
}

impl FieldType {
    fn size_of(&self) -> usize {
        use self::FieldType::*;
        match *self {
            Null => 0,
            U8 => 1,
            U16 => 2,
            U24 => 3,
            U32 => 4,
            U48 => 6,
            U64 => 8,
            F64 => 8,
            Zero => 0,
            One => 0,
            Blob(bytes) => bytes,
            Str(bytes) => bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum FieldValue<'a> {
    Null,
    Integer(u64),
    Float(f64),
    Blob(&'a [u8]),
    Str(&'a [u8]),
}

impl<'a> ToString for FieldValue<'a> {
    fn to_string(&self) -> String {
        use self::FieldValue::*;
        match *self {
            Null => "Null".to_owned(),
            Integer(i) => i.to_string(),
            Float(f) => f.to_string(),
            Blob(b) =>format!("{:?}", b),
            Str(s) => String::from_utf8_lossy(s).into_owned(),
        }
    }
}


#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Field<'a> {
    ty: FieldType,
    buffer: &'a [u8],
}

impl<'a> Field<'a> {
    fn value(&self) -> FieldValue<'a> {
        let buffer = self.buffer;
        match self.ty {
            FieldType::Null => FieldValue::Null,
            FieldType::U8 => FieldValue::Integer(buffer[0] as u64),
            FieldType::U16 => FieldValue::Integer(BigEndian::read_u16(buffer) as u64),
            FieldType::U24 => FieldValue::Integer(
                (((buffer[0] as u64) << 16) | ((buffer[1] as u64) << 8) | (buffer[2] as u64)) as
                    u64,
            ),
            FieldType::U32 => FieldValue::Integer(BigEndian::read_u32(buffer) as u64),
            FieldType::U48 => FieldValue::Integer(
                (((buffer[0] as u64) << 40) | ((buffer[1] as u64) << 32) |
                     ((buffer[2] as u64) << 24) | ((buffer[3] as u64) << 16) |
                     ((buffer[4] as u64) << 8) |
                     (buffer[5] as u64)) as u64,
            ),
            FieldType::U64 => FieldValue::Integer(BigEndian::read_u64(buffer)),
            FieldType::F64 => FieldValue::Float(BigEndian::read_f64(buffer)),
            FieldType::Zero => FieldValue::Integer(0 as u64),
            FieldType::One => FieldValue::Integer(1 as u64),
            FieldType::Blob(len) => FieldValue::Blob(&buffer[..len]),
            FieldType::Str(len) => FieldValue::Str(&buffer[..len]),
        }
    }
}

fn parse_record(record: &[u8]) -> Result<Vec<Field>> {
    let mut cursor = Cursor::new(record);
    let header_size = read_varint(&mut cursor)?;

    let mut field_types = Vec::new();
    while cursor.position() < header_size {
        let ty = match read_varint(&mut cursor)? {
            0 => FieldType::Null,
            1 => FieldType::U8,
            2 => FieldType::U16,
            3 => FieldType::U24,
            4 => FieldType::U32,
            5 => FieldType::U48,
            6 => FieldType::U64,
            7 => FieldType::F64,
            8 => FieldType::Zero,
            9 => FieldType::One,
            s @ 10 | s @ 11 => Err(format!("Use of reserved serial type {} in record", s))?,
            n if n > 12 && (n % 2 == 0) => FieldType::Blob(((n as usize) - 12) / 2),
            n if n > 13 && (n % 2 == 1) => FieldType::Str(((n as usize) - 13) / 2),
            // The compiler can't tell the above is an exhaustive match:
            _ => unreachable!()
        };
        field_types.push(ty);
    }

    let mut offset = cursor.position() as usize;
    let buffer = cursor.into_inner();
    Ok(field_types.into_iter().map(|ty| {
        let size_of = ty.size_of();
        let field = Field {
            ty,
            buffer: &buffer[offset..offset + size_of],
        };
        offset += size_of;
        field
    }).collect())
}


fn run() -> Result<()> {
    let mut file = File::open("aFile.db")?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let page = Page::new(&contents[..4096], 100)?;
    println!("Page type: {:?}", page.page_type());
    println!("Num cells: {:?}", page.num_cells());
    println!("Cell content size: {:?}", page.cell_contents().len());

    let mut cursor = Cursor::new(page.cell_contents());
    let payload_length = read_varint(&mut cursor)?;
    let rowid = read_varint(&mut cursor);
    let position = cursor.position() as usize;
    let fields = parse_record(&cursor.into_inner()[position..])?;
    println!("Len: {}", fields.len());
    println!("Data: {:#?}", fields.iter().map(|f| f.value().to_string()).collect::<Vec<_>>());

    // parse_record(&page.cell_contents()[2..]);

    Ok(())
}

quick_main!(run);

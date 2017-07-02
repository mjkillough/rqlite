use std::io::Cursor;
use std::io::prelude::*;
use std::string::ToString;

use bytes::Bytes;
use byteorder::{BigEndian, ByteOrder};

use errors::*;
use util::read_varint;


#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
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
pub enum FieldValue {
    Null,
    Integer(u64),
    Float(f64),
    Blob(Bytes),
    Str(Bytes),
}

// TODO: manual Debug instead?
impl ToString for FieldValue {
    fn to_string(&self) -> String {
        use self::FieldValue::*;
        match *self {
            Null => "Null".to_owned(),
            Integer(i) => i.to_string(),
            Float(f) => f.to_string(),
            Blob(ref b) => format!("{:?}", b),
            Str(ref s) => String::from_utf8_lossy(&s).into_owned(),
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    ty: FieldType,
    bytes: Bytes,
}

impl Field {
    pub fn value(&self) -> FieldValue {
        let bytes = &self.bytes;
        match self.ty {
            FieldType::Null => FieldValue::Null,
            FieldType::U8 => FieldValue::Integer(bytes[0] as u64),
            FieldType::U16 => FieldValue::Integer(BigEndian::read_u16(&bytes) as u64),
            FieldType::U24 => FieldValue::Integer(
                (((bytes[0] as u64) << 16) | ((bytes[1] as u64) << 8) | (bytes[2] as u64)) as u64,
            ),
            FieldType::U32 => FieldValue::Integer(BigEndian::read_u32(&bytes) as u64),
            FieldType::U48 => FieldValue::Integer(
                (((bytes[0] as u64) << 40) | ((bytes[1] as u64) << 32) |
                     ((bytes[2] as u64) << 24) | ((bytes[3] as u64) << 16) |
                     ((bytes[4] as u64) << 8) |
                     (bytes[5] as u64)) as u64,
            ),
            FieldType::U64 => FieldValue::Integer(BigEndian::read_u64(&bytes)),
            FieldType::F64 => FieldValue::Float(BigEndian::read_f64(&bytes)),
            FieldType::Zero => FieldValue::Integer(0 as u64),
            FieldType::One => FieldValue::Integer(1 as u64),
            FieldType::Blob(len) => FieldValue::Blob(bytes.clone()),
            FieldType::Str(len) => FieldValue::Str(bytes.clone()),
        }
    }
}


pub fn parse_record(bytes: Bytes) -> Result<Vec<Field>> {
    let mut cursor = Cursor::new(bytes);
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
            _ => unreachable!(),
        };
        field_types.push(ty);
    }

    let mut offset = cursor.position() as usize;
    let bytes = cursor.into_inner();
    Ok(
        field_types
            .into_iter()
            .map(|ty| {
                let size_of = ty.size_of();
                let field = Field {
                    ty,
                    bytes: bytes.slice(offset, offset + size_of),
                };
                offset += size_of;
                field
            })
            .collect(),
    )
}

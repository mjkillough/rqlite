use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::io::Cursor;
use std::io::prelude::*;
use std::ops::Index;
use std::result;
use std::slice;
use std::str;

use std::string::ToString;

use bytes::Bytes;
use byteorder::{BigEndian, ByteOrder};

use errors::*;
use types::Type;
use util::read_varint;


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
            Blob(len) => len,
            Str(len) => len,
        }
    }
}


#[derive(Clone, Debug)]
enum LiteralValue {
    Null,
    Integer(u64),
    Float(f64),
    Blob(Vec<u8>),
    Str(String),
}

#[derive(Clone, Debug)]
enum LazyValue {
    Blob(Bytes),
    Str(Bytes),
}

#[derive(Clone)]
pub enum Field {
    Lazy(LazyValue),
    Literal(LiteralValue),
}

impl Field {
    fn from_bytes(ty: FieldType, bytes: Bytes) -> Field {
        match ty {
            FieldType::Null => Field::Literal(LiteralValue::Null),
            FieldType::U8 => Field::Literal(LiteralValue::Integer(bytes[0] as u64)),
            FieldType::U16 => Field::Literal(
                LiteralValue::Integer(BigEndian::read_u16(&bytes) as u64),
            ),
            FieldType::U24 => Field::Literal(LiteralValue::Integer(
                (((bytes[0] as u64) << 16) | ((bytes[1] as u64) << 8) | (bytes[2] as u64)) as u64,
            )),
            FieldType::U32 => Field::Literal(
                LiteralValue::Integer(BigEndian::read_u32(&bytes) as u64),
            ),
            FieldType::U48 => Field::Literal(LiteralValue::Integer(
                (((bytes[0] as u64) << 40) | ((bytes[1] as u64) << 32) |
                     ((bytes[2] as u64) << 24) | ((bytes[3] as u64) << 16) |
                     ((bytes[4] as u64) << 8) |
                     (bytes[5] as u64)) as u64,
            )),
            FieldType::U64 => Field::Literal(LiteralValue::Integer(BigEndian::read_u64(&bytes))),
            FieldType::F64 => Field::Literal(LiteralValue::Float(BigEndian::read_f64(&bytes))),
            FieldType::Zero => Field::Literal(LiteralValue::Integer(0 as u64)),
            FieldType::One => Field::Literal(LiteralValue::Integer(1 as u64)),
            FieldType::Blob(_) => Field::Lazy(LazyValue::Blob(bytes)),
            FieldType::Str(_) => Field::Lazy(LazyValue::Str(bytes)),
        }
    }

    pub fn ty(&self) -> Type {
        match *self {
            Field::Literal(ref lit) => {
                match *lit {
                    LiteralValue::Null => Type::Null,
                    LiteralValue::Integer(_) => Type::Integer,
                    LiteralValue::Float(_) => Type::Float,
                    LiteralValue::Blob(_) => Type::Blob,
                    LiteralValue::Str(_) => Type::Text,
                }
            }
            Field::Lazy(ref lazy) => {
                match *lazy {
                    LazyValue::Blob(_) => Type::Blob,
                    LazyValue::Str(_) => Type::Text,
                }
            }
        }
    }

    pub fn as_null(&self) -> Result<()> {
        match self.ty() {
            Type::Null => Ok(()),
            ty => Err(ErrorKind::UnexpectedType(Type::Null, ty).into()),
        }
    }

    pub fn as_integer(&self) -> Result<u64> {
        match *self {
            Field::Literal(LiteralValue::Integer(i)) => Ok(i),
            _ => Err(ErrorKind::UnexpectedType(Type::Integer, self.ty()).into()),
        }
    }

    pub fn as_float(&self) -> Result<f64> {
        match *self {
            Field::Literal(LiteralValue::Float(f)) => Ok(f),
            _ => Err(ErrorKind::UnexpectedType(Type::Float, self.ty()).into()),
        }
    }

    pub fn as_blob(&self) -> Result<&[u8]> {
        match *self {
            Field::Literal(LiteralValue::Blob(ref vec)) => Ok(vec),
            Field::Lazy(LazyValue::Blob(ref bytes)) => Ok(bytes),
            _ => Err(ErrorKind::UnexpectedType(Type::Blob, self.ty()).into()),
        }
    }

    pub fn as_text(&self) -> Result<&str> {
        match *self {
            Field::Literal(LiteralValue::Str(ref string)) => Ok(&string),
            // XXX Not necessarily UTF-8.
            Field::Lazy(LazyValue::Str(ref bytes)) => Ok(str::from_utf8(bytes)?),
            _ => Err(ErrorKind::UnexpectedType(Type::Text, self.ty()).into()),
        }
    }
}

impl From<u64> for Field {
    fn from(value: u64) -> Field {
        Field::Literal(LiteralValue::Integer(value))
    }
}

impl<'a> From<&'a str> for Field {
    fn from(value: &str) -> Field {
        Field::Literal(LiteralValue::Str(value.to_owned()))
    }
}

// TODO: Implement the proper affinity rules for types.
impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        let result = match self.ty() {
            Type::Null => other.as_null().map(|_| true),
            Type::Integer => other.as_integer().map(|o| self.as_integer().unwrap() == o),
            Type::Float => other.as_float().map(|o| self.as_float().unwrap() == o),
            Type::Blob => other.as_blob().map(|o| self.as_blob().unwrap() == o),
            Type::Text => other.as_text().map(|o| self.as_text().unwrap() == o),
        };
        result.expect("Unimplemented: proper affinity types in Field comparisons")
    }
}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Field) -> Option<Ordering> {
        let result = match self.ty() {
            Type::Null => other.as_null().map(|_| Some(Ordering::Equal)),
            Type::Integer => {
                other
                    .as_integer()
                    .map(|o| self.as_integer().unwrap().partial_cmp(&o))
            }
            Type::Float => {
                other
                    .as_float()
                    .map(|o| self.as_float().unwrap().partial_cmp(&o))
            }
            Type::Blob => {
                other
                    .as_blob()
                    .map(|o| self.as_blob().unwrap().partial_cmp(o))
            }
            Type::Text => {
                other
                    .as_text()
                    .map(|o| self.as_text().unwrap().partial_cmp(o))
            }
        };
        result.expect("Unimplemented: proper affinity types in Field comparisons")
    }
}

impl fmt::Debug for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match *self {
            Field::Literal(ref lit) => {
                match *lit {
                    LiteralValue::Null => write!(f, "null"),
                    LiteralValue::Integer(i) => write!(f, "{}", i),
                    LiteralValue::Float(float) => write!(f, "{}", float),
                    LiteralValue::Blob(ref vec) => write!(f, "{:?}", vec),
                    LiteralValue::Str(ref string) => write!(f, "\"{}\"", string),
                }
            }
            Field::Lazy(ref lazy) => {
                match *lazy {
                    LazyValue::Blob(ref bytes) => write!(f, "{:?}", bytes),
                    // XXX Not necessarily UTF-8.
                    LazyValue::Str(ref bytes) => {
                        write!(f, "\"{}\"", String::from_utf8_lossy(bytes))
                    }
                }
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct Record {
    fields: Vec<Field>,
}

impl Record {
    pub fn new(fields: Vec<Field>) -> Record {
        Record { fields }
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Record> {
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
        let fields = field_types
            .into_iter()
            .map(|ty| {
                let size_of = ty.size_of();
                let field = Field::from_bytes(ty, bytes.slice(offset, offset + size_of));
                offset += size_of;
                field
            })
            .collect();

        Ok(Record { fields })
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn iter(&self) -> slice::Iter<Field> {
        self.fields.iter()
    }
}

impl Index<usize> for Record {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

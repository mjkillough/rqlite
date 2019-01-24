use std::io::prelude::*;

use errors::*;

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
pub fn read_varint<R: Read>(mut data: R) -> Result<u64> {
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

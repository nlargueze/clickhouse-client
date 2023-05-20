//! RowBinary deserialization

use std::io::Read;

use serde::{de::Visitor, Deserializer};

use crate::error::Error;

/// Deserializer for the `RowBin` format
///
/// # Notes
///
/// The Clone and Copy are shallow copies
#[derive(Debug, Clone, Copy)]
pub struct RowBinDeserializer<'a> {
    /// Buffer
    pub buffer: &'a [u8],
}

impl<'a> RowBinDeserializer<'a> {
    /// Deserializes a slice of bytes
    pub fn new(bytes: &'a [u8]) -> Self {
        RowBinDeserializer { buffer: bytes }
    }

    /// Returns the remaining bytes
    pub fn remaining(&self) -> &'a [u8] {
        self.buffer
    }
}

impl<'de> Deserializer<'de> for RowBinDeserializer<'de> {
    type Error = Error;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::new("any value not supported by RowBinDeserializer"))
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_bool = [0u8; 1];
        self.buffer.read_exact(&mut buf_bool)?;
        let b = match buf_bool[0] {
            0x00 => false,
            0x01 => true,
            _ => return Err(Error::new("invalid boolean")),
        };
        visitor.visit_bool(b)
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_i8 = [0u8; 1];
        self.buffer.read_exact(&mut buf_i8)?;
        let i = i8::from_le_bytes(buf_i8);
        visitor.visit_i8(i)
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_i16 = [0u8; 2];
        self.buffer.read_exact(&mut buf_i16)?;
        let i = i16::from_le_bytes(buf_i16);
        visitor.visit_i16(i)
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_i32 = [0u8; 4];
        self.buffer.read_exact(&mut buf_i32)?;
        let i = i32::from_le_bytes(buf_i32);
        visitor.visit_i32(i)
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_i64 = [0u8; 8];
        self.buffer.read_exact(&mut buf_i64)?;
        let i = i64::from_le_bytes(buf_i64);
        visitor.visit_i64(i)
    }

    fn deserialize_i128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_i128 = [0u8; 16];
        self.buffer.read_exact(&mut buf_i128)?;
        let i = i128::from_le_bytes(buf_i128);
        visitor.visit_i128(i)
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_u8 = [0u8; 1];
        self.buffer.read_exact(&mut buf_u8)?;
        let i = u8::from_le_bytes(buf_u8);
        visitor.visit_u8(i)
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_u16 = [0u8; 2];
        self.buffer.read_exact(&mut buf_u16)?;
        let i = u16::from_le_bytes(buf_u16);
        visitor.visit_u16(i)
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_u32 = [0u8; 4];
        self.buffer.read_exact(&mut buf_u32)?;
        let i = u32::from_le_bytes(buf_u32);
        visitor.visit_u32(i)
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_u64 = [0u8; 8];
        self.buffer.read_exact(&mut buf_u64)?;
        let i = u64::from_le_bytes(buf_u64);
        visitor.visit_u64(i)
    }

    fn deserialize_u128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_u128 = [0u8; 16];
        self.buffer.read_exact(&mut buf_u128)?;
        let i = u128::from_le_bytes(buf_u128);
        visitor.visit_u128(i)
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_f32 = [0u8; 4];
        self.buffer.read_exact(&mut buf_f32)?;
        let f = f32::from_le_bytes(buf_f32);
        visitor.visit_f32(f)
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_f64 = [0u8; 8];
        self.buffer.read_exact(&mut buf_f64)?;
        let f = f64::from_le_bytes(buf_f64);
        visitor.visit_f64(f)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        eprintln!("deserialize_string: {:0X?}", self.buffer);
        let n: usize = leb128::read::unsigned(&mut self.buffer)?.try_into()?;
        let mut buf_str = vec![0u8; n];
        self.buffer.read_exact(&mut buf_str)?;
        let string = String::from_utf8(buf_str)?;
        eprintln!("self.buffer={:0X?}", self.buffer);
        visitor.visit_string(string)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(self.buffer)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf_opt = [0u8; 1];
        self.buffer.read_exact(&mut buf_opt)?;
        match buf_opt[0] {
            0x00 => {
                // => NOT NULL
                visitor.visit_some(self)
            }
            0x01 => {
                // => NULL
                visitor.visit_none()
            }
            _ => Err(Error::new("option byte must be either 0 or 1")),
        }
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::new("unit value not supported by RowBinDeserializer"))
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::new(
            "unit_struct value not supported by RowBinDeserializer",
        ))
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::new(
            "newtype_struct value not supported by RowBinDeserializer",
        ))
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_seq")
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_tuple")
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_tuple_struct")
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_map")
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_struct")
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_struct")
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_identifier")
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("deserialize_ignored_any")
    }
}

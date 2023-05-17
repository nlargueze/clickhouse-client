//! RowBin format

use std::{
    collections::HashMap,
    io::{Read, Write},
    str::FromStr,
};

use ethnum::{I256, U256};
use uuid::Uuid;

use crate::{
    error::Error,
    query::QueryData,
    value::{Type, Value},
};

use super::Formatter;

#[cfg(test)]
mod tests;

/// RowBinary formatter
#[derive(Debug, Clone, Default)]
pub struct RowBinFormatter {
    /// Has column names
    with_names: bool,
    /// With column types
    with_types: bool,
}

impl RowBinFormatter {
    /// Creates a new [RowBinaryFormatter]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new [RowBinaryFormatter] with names
    pub fn with_names() -> Self {
        Self {
            with_names: true,
            with_types: false,
        }
    }

    /// Creates a new [RowBinaryFormatter] with names and types
    pub fn with_names_and_types() -> Self {
        Self {
            with_names: true,
            with_types: true,
        }
    }
}

impl Formatter for RowBinFormatter {
    fn serialize_value(&self, value: Value) -> Vec<u8> {
        self.format_value(value)
    }

    fn serialize_query_data(&self, data: QueryData) -> Result<Vec<u8>, Error> {
        self.format_data(data)
    }

    fn deserialize_value(&self, bytes: &[u8], ty: Type) -> Result<Value, Error> {
        let mut bytes = bytes;
        let value = self.parse_value(&mut bytes, ty)?;
        if !bytes.is_empty() {
            return Err(Error::new("Value bytes has remaining bytes"));
        }
        Ok(value)
    }

    fn deserialize_query_data(
        &self,
        bytes: &[u8],
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<QueryData, Error> {
        let mut bytes = bytes;
        self.parse_data(&mut bytes, mapping)
    }
}

impl RowBinFormatter {
    /// Formats a value
    #[allow(clippy::only_used_in_recursion)]
    fn format_value(&self, value: Value) -> Vec<u8> {
        /// Implements the nullable variant for formatting
        macro_rules! impl_nullable {
            ($VAL:tt, $VAR:ident) => {
                match $VAL {
                    Some(v) => {
                        let mut buf = vec![0x00];
                        let mut bytes = self.format_value(Value::$VAR(v));
                        buf.append(&mut bytes);
                        buf
                    }
                    None => vec![0x01],
                }
            };
        }

        match value {
            Value::UInt8(v) => v.to_le_bytes().to_vec(),
            Value::UInt16(v) => v.to_le_bytes().to_vec(),
            Value::UInt32(v) => v.to_le_bytes().to_vec(),
            Value::UInt64(v) => v.to_le_bytes().to_vec(),
            Value::UInt128(v) => v.to_le_bytes().to_vec(),
            Value::UInt256(_) => {
                let u256: U256 = value.try_into().unwrap();
                u256.to_le_bytes().to_vec()
            }
            Value::Int8(v) => v.to_le_bytes().to_vec(),
            Value::Int16(v) => v.to_le_bytes().to_vec(),
            Value::Int32(v) => v.to_le_bytes().to_vec(),
            Value::Int64(v) => v.to_le_bytes().to_vec(),
            Value::Int128(v) => v.to_le_bytes().to_vec(),
            Value::Int256(_) => {
                let i256: I256 = value.try_into().unwrap();
                i256.to_le_bytes().to_vec()
            }
            Value::Float32(v) => v.to_le_bytes().to_vec(),
            Value::Float64(v) => v.to_le_bytes().to_vec(),
            Value::Bool(v) => {
                if v {
                    vec![0x01]
                } else {
                    vec![0x00]
                }
            }
            Value::String(v) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, v.len() as u64).unwrap();
                buf.write_all(v.as_bytes()).unwrap();
                buf
            }
            Value::UUID(v) => {
                // NB: in RowBinary, the UUID is represented as 2 u64 in little endian
                let (w1, w2) = Uuid::from_bytes(v).as_u64_pair();
                let mut buf = w1.to_le_bytes().to_vec();
                buf.append(&mut w2.to_le_bytes().to_vec());
                buf
            }
            Value::Date(v) => v.to_le_bytes().to_vec(),
            Value::Date32(v) => v.to_le_bytes().to_vec(),
            Value::DateTime(v) => v.to_le_bytes().to_vec(),
            Value::DateTime64(v) => v.to_le_bytes().to_vec(),
            Value::Enum8(v) => v.to_le_bytes().to_vec(),
            Value::Enum16(v) => v.to_le_bytes().to_vec(),
            Value::Array(v) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, v.len() as u64).unwrap();
                let values = v
                    .into_iter()
                    .flat_map(|value| self.format_value(value))
                    .collect::<Vec<_>>();
                buf.write_all(&values).unwrap();
                buf
            }
            Value::Tuple(v) => v
                .into_iter()
                .flat_map(|value| self.format_value(value))
                .collect::<Vec<_>>(),
            Value::Map(map) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, map.len() as u64).unwrap();
                for (k, v) in map {
                    let mut key_bytes = self.format_value(Value::String(k));
                    let mut val_bytes = self.format_value(v);
                    buf.append(&mut key_bytes);
                    buf.append(&mut val_bytes);
                }
                buf
            }
            Value::Nested(fields) => {
                let mut buf = vec![];
                for (k, v) in fields {
                    let mut key_bytes = self.format_value(Value::String(k));
                    let mut val_bytes = self.format_value(v);
                    buf.append(&mut key_bytes);
                    buf.append(&mut val_bytes);
                }
                buf
            }
            Value::NullableUInt8(v) => impl_nullable!(v, UInt8),
            Value::NullableUInt16(v) => impl_nullable!(v, UInt16),
            Value::NullableUInt32(v) => impl_nullable!(v, UInt32),
            Value::NullableUInt64(v) => impl_nullable!(v, UInt64),
            Value::NullableUInt128(v) => impl_nullable!(v, UInt128),
            Value::NullableUInt256(v) => impl_nullable!(v, UInt256),
            Value::NullableInt8(v) => impl_nullable!(v, Int8),
            Value::NullableInt16(v) => impl_nullable!(v, Int16),
            Value::NullableInt32(v) => impl_nullable!(v, Int32),
            Value::NullableInt64(v) => impl_nullable!(v, Int64),
            Value::NullableInt128(v) => impl_nullable!(v, Int128),
            Value::NullableInt256(v) => impl_nullable!(v, Int256),
            Value::NullableFloat32(v) => impl_nullable!(v, Float32),
            Value::NullableFloat64(v) => impl_nullable!(v, Float64),
            Value::NullableBool(v) => impl_nullable!(v, Bool),
            Value::NullableString(v) => impl_nullable!(v, String),
            Value::NullableUUID(v) => impl_nullable!(v, UUID),
            Value::NullableDate(v) => impl_nullable!(v, Date),
            Value::NullableDate32(v) => impl_nullable!(v, Date32),
            Value::NullableDateTime(v) => impl_nullable!(v, DateTime),
            Value::NullableDateTime64(v) => impl_nullable!(v, DateTime64),
            Value::NullableEnum8(v) => impl_nullable!(v, Enum8),
            Value::NullableEnum16(v) => impl_nullable!(v, Enum16),
        }
    }

    /// Formats a table
    fn format_data(&self, data: QueryData) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];
        let parts = data.into_parts();

        // column names
        if self.with_names {
            if let Some(names) = parts.names {
                leb128::write::unsigned(&mut buf, names.len().try_into()?).unwrap();
                for name in names {
                    let bytes = self.format_value(Value::String(name));
                    buf.write_all(&bytes)?;
                }
            } else {
                return Err(Error::new("Table is missing the column names"));
            }
        }

        // column types
        if self.with_types {
            if let Some(types) = parts.types {
                let types = types.into_iter().map(|t| t.to_string()).collect::<Vec<_>>();
                leb128::write::unsigned(&mut buf, types.len().try_into()?).unwrap();
                for ty in types {
                    let bytes = self.format_value(Value::String(ty));
                    buf.write_all(&bytes)?;
                }
            } else {
                return Err(Error::new("Table is missing the column types"));
            }
        }

        for row in parts.rows {
            for value in row {
                let bytes = self.format_value(value);
                buf.write_all(&bytes)?;
            }
        }

        Ok(buf)
    }

    /// Parses a value
    fn parse_value(&self, bytes: &mut &[u8], ty: Type) -> Result<Value, Error> {
        /// Implements the nullable variant for parsing
        macro_rules! impl_nullable {
            ($NULL_TY:tt, $TY:expr) => {{
                let mut buf = [0x00_u8; 1];
                bytes.read_exact(&mut buf)?;
                match buf {
                    [0x01] => Ok(Value::$NULL_TY(None)),
                    [0x00] => match self.parse_value(bytes, $TY)?.into_nullable() {
                        Some(v) => Ok(v),
                        None => Err(Error::new("Invalid nullable value")),
                    },
                    _ => Err(Error::new("Invalid nullable value")),
                }
            }};
        }

        match ty {
            Type::UInt8 => {
                let mut buf = [0x00_u8; 1];
                bytes.read_exact(&mut buf)?;
                let v = u8::from_le_bytes(buf);
                Ok(Value::UInt8(v))
            }
            Type::UInt16 => {
                let mut buf = [0x00_u8; 2];
                bytes.read_exact(&mut buf)?;
                let v = u16::from_le_bytes(buf);
                Ok(Value::UInt16(v))
            }
            Type::UInt32 => {
                let mut buf = [0x00_u8; 4];
                bytes.read_exact(&mut buf)?;
                let v = u32::from_le_bytes(buf);
                Ok(Value::UInt32(v))
            }
            Type::UInt64 => {
                let mut buf = [0x00_u8; 8];
                bytes.read_exact(&mut buf)?;
                let v = u64::from_le_bytes(buf);
                Ok(Value::UInt64(v))
            }
            Type::UInt128 => {
                let mut buf = [0x00_u8; 16];
                bytes.read_exact(&mut buf)?;
                let v = u128::from_le_bytes(buf);
                Ok(Value::UInt128(v))
            }
            Type::UInt256 => {
                let mut buf = [0x00_u8; 32];
                bytes.read_exact(&mut buf)?;
                let v = U256::from_le_bytes(buf);
                Ok(Value::UInt256(v.into_words().into()))
            }
            Type::Int8 => {
                let mut buf = [0x00_u8; 1];
                bytes.read_exact(&mut buf)?;
                let v = i8::from_le_bytes(buf);
                Ok(Value::Int8(v))
            }
            Type::Int16 => {
                let mut buf = [0x00_u8; 2];
                bytes.read_exact(&mut buf)?;
                let v = i16::from_le_bytes(buf);
                Ok(Value::Int16(v))
            }
            Type::Int32 => {
                let mut buf = [0x00_u8; 4];
                bytes.read_exact(&mut buf)?;
                let v = i32::from_le_bytes(buf);
                Ok(Value::Int32(v))
            }
            Type::Int64 => {
                let mut buf = [0x00_u8; 8];
                bytes.read_exact(&mut buf)?;
                let v = i64::from_le_bytes(buf);
                Ok(Value::Int64(v))
            }
            Type::Int128 => {
                let mut buf = [0x00_u8; 16];
                bytes.read_exact(&mut buf)?;
                let v = i128::from_le_bytes(buf);
                Ok(Value::Int128(v))
            }
            Type::Int256 => {
                let mut buf = [0x00_u8; 32];
                bytes.read_exact(&mut buf)?;
                let v = I256::from_le_bytes(buf);
                Ok(Value::Int256(v.into_words().into()))
            }
            Type::Float32 => {
                let mut buf = [0x00_u8; 4];
                bytes.read_exact(&mut buf)?;
                let v = f32::from_le_bytes(buf);
                Ok(Value::Float32(v))
            }
            Type::Float64 => {
                let mut buf = [0x00_u8; 8];
                bytes.read_exact(&mut buf)?;
                let v = f64::from_le_bytes(buf);
                Ok(Value::Float64(v))
            }
            Type::Decimal(_, _) => {
                unimplemented!("RowBinary format Decimal")
            }
            Type::Decimal32(_) => {
                unimplemented!("RowBinary format Decimal32")
            }
            Type::Decimal64(_) => {
                unimplemented!("RowBinary format Decimal64")
            }
            Type::Decimal128(_) => {
                unimplemented!("RowBinary format Decimal128")
            }
            Type::Decimal256(_) => {
                unimplemented!("RowBinary format Decimal256")
            }
            Type::Bool => {
                let mut buf = [0x00_u8; 1];
                bytes.read_exact(&mut buf)?;
                match buf {
                    [0x00] => Ok(Value::Bool(false)),
                    [0x01] => Ok(Value::Bool(true)),
                    _ => Err(Error::new("Invalid bool value")),
                }
            }
            Type::String => {
                let n: usize = leb128::read::unsigned(bytes)?.try_into()?;
                let mut buf = vec![0x00_u8; n];
                bytes.read_exact(&mut buf)?;
                let s = String::from_utf8(buf)?;
                Ok(Value::String(s))
            }
            Type::FixedString(n) => {
                let mut buf = vec![0x00_u8; n.into()];
                bytes.read_exact(&mut buf)?;
                let s = String::from_utf8(buf)?;
                Ok(Value::String(s))
            }
            Type::UUID => {
                // NB: in RowBinary, the UUID is represented as 2 u64 in little endian
                let mut buf = [0x00_u8; 8];
                bytes.read_exact(&mut buf)?;
                let w1 = u64::from_le_bytes(buf);
                bytes.read_exact(&mut buf)?;
                let w2 = u64::from_le_bytes(buf);
                let uuid = Uuid::from_u64_pair(w1, w2);
                Ok(Value::UUID(uuid.into_bytes()))
            }
            Type::Date => {
                let mut buf = [0x00_u8; 2];
                bytes.read_exact(&mut buf)?;
                let v = u16::from_le_bytes(buf);
                Ok(Value::Date(v))
            }
            Type::Date32 => {
                let mut buf = [0x00_u8; 4];
                bytes.read_exact(&mut buf)?;
                let v = i32::from_le_bytes(buf);
                Ok(Value::Date32(v))
            }
            Type::DateTime => {
                let mut buf = [0x00_u8; 4];
                bytes.read_exact(&mut buf)?;
                let v = u32::from_le_bytes(buf);
                Ok(Value::DateTime(v))
            }
            Type::DateTime64(_) => {
                let mut buf = [0x00_u8; 8];
                bytes.read_exact(&mut buf)?;
                let v = i64::from_le_bytes(buf);
                Ok(Value::DateTime64(v))
            }
            Type::Enum8(_) => {
                let mut buf = [0x00_u8; 1];
                bytes.read_exact(&mut buf)?;
                let v = i8::from_le_bytes(buf);
                Ok(Value::Enum8(v))
            }
            Type::Enum16(_) => {
                let mut buf = [0x00_u8; 2];
                bytes.read_exact(&mut buf)?;
                let v = i16::from_le_bytes(buf);
                Ok(Value::Enum16(v))
            }
            Type::Array(ty) => {
                let mut values = vec![];
                let n = leb128::read::unsigned(bytes)?;
                for _ in 0..n {
                    let value = self.parse_value(bytes, (*ty).clone())?;
                    values.push(value);
                }
                Ok(Value::Array(values))
            }
            Type::Tuple(types) => {
                let mut values = vec![];
                for ty in types {
                    let value = self.parse_value(bytes, ty)?;
                    values.push(value);
                }
                Ok(Value::Tuple(values))
            }
            Type::Map(_ty_key, ty_val) => {
                let mut map = HashMap::new();
                let n = leb128::read::unsigned(bytes)?;
                for _ in 0..n {
                    let key = self.parse_value_str(bytes)?;
                    let value = self.parse_value(bytes, (*ty_val).clone())?;
                    map.insert(key, value);
                }
                Ok(Value::Map(map))
            }
            Type::Nested(fields) => {
                let mut map = HashMap::new();
                for (_name, ty) in fields {
                    let key = self.parse_value_str(bytes)?;
                    let value = self.parse_value(bytes, ty)?;
                    map.insert(key, value);
                }
                Ok(Value::Nested(map))
            }
            Type::NullableUInt8 => impl_nullable!(NullableUInt8, Type::UInt8),
            Type::NullableUInt16 => impl_nullable!(NullableUInt16, Type::UInt16),
            Type::NullableUInt32 => impl_nullable!(NullableUInt32, Type::UInt32),
            Type::NullableUInt64 => impl_nullable!(NullableUInt64, Type::UInt64),
            Type::NullableUInt128 => impl_nullable!(NullableUInt128, Type::UInt128),
            Type::NullableUInt256 => impl_nullable!(NullableUInt256, Type::UInt256),
            Type::NullableInt8 => impl_nullable!(NullableInt8, Type::Int8),
            Type::NullableInt16 => impl_nullable!(NullableInt16, Type::Int16),
            Type::NullableInt32 => impl_nullable!(NullableInt32, Type::Int32),
            Type::NullableInt64 => impl_nullable!(NullableInt64, Type::Int64),
            Type::NullableInt128 => impl_nullable!(NullableInt128, Type::Int128),
            Type::NullableInt256 => impl_nullable!(NullableInt256, Type::Int256),
            Type::NullableFloat32 => impl_nullable!(NullableFloat32, Type::Float32),
            Type::NullableFloat64 => impl_nullable!(NullableFloat64, Type::Float64),
            Type::NullableDecimal(_, _) => unimplemented!("RowBinary format Decimal"),
            Type::NullableDecimal32(_) => unimplemented!("RowBinary format Decimal32"),
            Type::NullableDecimal64(_) => unimplemented!("RowBinary format Decimal64"),
            Type::NullableDecimal128(_) => unimplemented!("RowBinary format Decimal128"),
            Type::NullableDecimal256(_) => unimplemented!("RowBinary format Decimal256"),
            Type::NullableBool => impl_nullable!(NullableBool, Type::Bool),
            Type::NullableString => impl_nullable!(NullableString, Type::String),
            Type::NullableFixedString(n) => impl_nullable!(NullableString, Type::FixedString(n)),
            Type::NullableUUID => impl_nullable!(NullableUUID, Type::UUID),
            Type::NullableDate => impl_nullable!(NullableDate, Type::Date),
            Type::NullableDate32 => impl_nullable!(NullableDate32, Type::Date32),
            Type::NullableDateTime => impl_nullable!(NullableDateTime, Type::DateTime),
            Type::NullableDateTime64(p) => impl_nullable!(NullableDateTime64, Type::DateTime64(p)),
            Type::NullableEnum8(variants) => impl_nullable!(NullableEnum8, Type::Enum8(variants)),
            Type::NullableEnum16(variants) => {
                impl_nullable!(NullableEnum16, Type::Enum16(variants))
            }
        }
    }

    /// Parses a value as a string
    fn parse_value_str(&self, bytes: &mut &[u8]) -> Result<String, Error> {
        let n: usize = leb128::read::unsigned(bytes)?.try_into()?;
        let mut buf = vec![0x00_u8; n];
        bytes.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    fn parse_data(
        &self,
        bytes: &mut &[u8],
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<QueryData, Error> {
        // parse names + types from the buffer
        let mut data = if self.with_names {
            let n = leb128::read::unsigned(bytes).unwrap().try_into()?;
            let mut names = vec![];
            for _i in 0..n {
                let name = self.parse_value_str(bytes)?;
                names.push(name);
            }

            if self.with_types {
                let mut types = vec![];
                let mut names_and_types = vec![];
                for i in 0..n {
                    let ty_str = self.parse_value_str(bytes)?;
                    let ty = Type::from_str(&ty_str)?;
                    types.push(ty.clone());
                    let name = names.get(i).ok_or(Error::new("Missing column name"))?;
                    names_and_types.push((name.as_str(), ty));
                }
                QueryData::with_names_and_types(names_and_types)
            } else {
                let names = names.iter().map(String::as_str).collect();
                QueryData::with_names(names)
            }
        } else {
            QueryData::no_headers()
        };

        // parse rows from the buffer
        let types = if let Some(types) = data.get_types() {
            types
        } else if let Some(mapping) = mapping {
            mapping.iter().map(|(_, t)| t.clone()).collect()
        } else {
            return Err(Error::new("Deserializing data requires a mapping table"));
        };

        // parse rows
        while !bytes.is_empty() {
            // loop on each columns
            let mut row = vec![];
            for ty in &types {
                let value = self.parse_value(bytes, ty.clone())?;
                row.push(value);
            }
            data.add_row(row);
        }

        Ok(data)
    }
}

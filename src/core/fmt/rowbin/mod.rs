//! RowBinary format

#[cfg(test)]
mod tests;

use std::io::{Cursor, Read, Write};

use hyper::body::Buf;

use crate::{
    core::{Type, Value},
    error::Error,
    query::QueryTable,
};

use super::Formatter;

/// Formatter for RowBinary format
#[derive(Debug, Default)]
pub struct RowBinFormatter;

impl RowBinFormatter {
    /// Creates a new formatter
    pub fn new() -> Self {
        Self::default()
    }
}

impl Formatter for RowBinFormatter {
    type Ok = Vec<u8>;

    type Err = Error;

    fn format(&self, value: &Value) -> Self::Ok {
        match value {
            Value::UInt8(u) => u.to_le_bytes().to_vec(),
            Value::UInt16(u) => u.to_le_bytes().to_vec(),
            Value::UInt32(u) => u.to_le_bytes().to_vec(),
            Value::UInt64(u) => u.to_le_bytes().to_vec(),
            Value::UInt128(u) => u.to_le_bytes().to_vec(),
            Value::UInt256(u) => u.to_vec(),
            Value::Int8(i) => i.to_le_bytes().to_vec(),
            Value::Int16(i) => i.to_le_bytes().to_vec(),
            Value::Int32(i) => i.to_le_bytes().to_vec(),
            Value::Int64(i) => i.to_le_bytes().to_vec(),
            Value::Int128(i) => i.to_le_bytes().to_vec(),
            Value::Int256(i) => i.to_vec(),
            Value::Float32(f) => f.to_le_bytes().to_vec(),
            Value::Float64(f) => f.to_le_bytes().to_vec(),
            Value::Decimal32(d) => d.to_le_bytes().to_vec(),
            Value::Decimal64(d) => d.to_le_bytes().to_vec(),
            Value::Decimal128(d) => d.to_le_bytes().to_vec(),
            Value::Bool(b) => match b {
                false => vec![0x00],
                true => vec![0x01],
            },
            Value::String(s) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, s.len() as u64).unwrap();
                buf.write_all(s.as_bytes()).unwrap();
                buf
            }
            Value::FixedString(s) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, s.len() as u64).unwrap();
                buf.write_all(s.as_bytes()).unwrap();
                buf
            }
            Value::Date(d) => d.to_le_bytes().to_vec(),
            Value::Date32(d) => d.to_le_bytes().to_vec(),
            Value::DateTime(dt) => dt.to_le_bytes().to_vec(),
            Value::DateTime64(dt) => dt.to_le_bytes().to_vec(),
            Value::UUID(id) => id.to_vec(),
            Value::Enum8(_) => todo!("format enum8"),
            Value::Enum16(_) => todo!("format enum16"),
            Value::Array(_) => todo!("format array"),
            Value::Map(_) => todo!("format map"),
            Value::Nested(_) => todo!("format nested"),
            Value::NullableUInt8(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt8(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUInt16(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt16(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUInt32(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt32(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUInt64(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt64(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUInt128(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt128(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUInt256(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UInt256(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt8(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int8(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt16(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int16(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt32(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int32(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt64(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int64(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt128(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int128(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableInt256(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Int256(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableFloat32(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Float32(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableFloat64(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Float64(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDecimal32(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Decimal32(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDecimal64(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Decimal64(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDecimal128(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Decimal128(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableBool(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Bool(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableString(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::String(x.clone()));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableFixedString(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::FixedString(x.clone()));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDate(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Date(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDate32(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::Date32(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDateTime(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::DateTime(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableDateTime64(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::DateTime64(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
            Value::NullableUUID(x) => match x {
                Some(x) => {
                    let mut buf = vec![0x00];
                    let mut v = self.format(&Value::UUID(*x));
                    buf.append(&mut v);
                    buf
                }
                None => {
                    vec![0x01]
                }
            },
        }
    }

    /// Parses a [Value] from a reader and a type
    fn parse(&self, ty: Type, reader: &mut impl Read) -> Result<Value, Self::Err> {
        match ty {
            Type::UInt8 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt8(buf[0]))
            }
            Type::UInt16 => {
                let mut buf = [0x00_u8; 2];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt16(u16::from_le_bytes(buf)))
            }
            Type::UInt32 => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt32(u32::from_le_bytes(buf)))
            }
            Type::UInt64 => {
                let mut buf = [0x00_u8; 8];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt64(u64::from_le_bytes(buf)))
            }
            Type::UInt128 => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt128(u128::from_le_bytes(buf)))
            }
            Type::UInt256 => {
                let mut buf = [0x00_u8; 32];
                reader.read_exact(&mut buf)?;
                Ok(Value::UInt256(buf))
            }
            Type::Int8 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int8(i8::from_le_bytes(buf)))
            }
            Type::Int16 => {
                let mut buf = [0x00_u8; 2];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int16(i16::from_le_bytes(buf)))
            }
            Type::Int32 => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int32(i32::from_le_bytes(buf)))
            }
            Type::Int64 => {
                let mut buf = [0x00_u8; 8];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int64(i64::from_le_bytes(buf)))
            }
            Type::Int128 => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int128(i128::from_le_bytes(buf)))
            }
            Type::Int256 => {
                let mut buf = [0x00_u8; 32];
                reader.read_exact(&mut buf)?;
                Ok(Value::Int256(buf))
            }
            Type::Float32 => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::Float32(f32::from_le_bytes(buf)))
            }
            Type::Float64 => {
                let mut buf = [0x00_u8; 8];
                reader.read_exact(&mut buf)?;
                Ok(Value::Float64(f64::from_le_bytes(buf)))
            }
            Type::Decimal(_, _) => {
                unimplemented!("Decimal value");
            }
            Type::Decimal32(_) => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::Decimal32(i32::from_le_bytes(buf)))
            }
            Type::Decimal64(_) => {
                let mut buf = [0x00_u8; 8];
                reader.read_exact(&mut buf)?;
                Ok(Value::Decimal64(i64::from_le_bytes(buf)))
            }
            Type::Decimal128(_) => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::Decimal128(i128::from_le_bytes(buf)))
            }
            Type::Decimal256(_) => {
                unimplemented!("Decimal256 value")
            }
            Type::Bool => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                let b = match buf[0] {
                    0x00 => false,
                    0x01 => true,
                    _ => {
                        return Err(Error::new("invalid boolean value"));
                    }
                };
                Ok(Value::Bool(b))
            }
            Type::String => {
                let n: usize = leb128::read::unsigned(reader)?.try_into()?;
                let mut buf = vec![0u8; n];
                reader.read_exact(&mut buf)?;
                let s = String::from_utf8(buf)?;
                Ok(Value::String(s))
            }
            Type::FixedString(n) => {
                let mut buf = vec![0u8; n];
                reader.read_exact(&mut buf)?;
                let s: String = String::from_utf8(buf)?;
                Ok(Value::FixedString(s))
            }
            Type::Date => {
                let mut buf = [0x00_u8; 2];
                reader.read_exact(&mut buf)?;
                Ok(Value::Date(u16::from_le_bytes(buf)))
            }
            Type::Date32 => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::Date32(i32::from_le_bytes(buf)))
            }
            Type::DateTime => {
                let mut buf = [0x00_u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(Value::DateTime(u32::from_le_bytes(buf)))
            }
            Type::DateTime64(_) => {
                let mut buf = [0x00_u8; 8];
                reader.read_exact(&mut buf)?;
                Ok(Value::DateTime64(i64::from_le_bytes(buf)))
            }
            Type::Enum8(_) => todo!("enum parsing"),
            Type::Enum16(_) => todo!("enum parsing"),
            Type::UUID => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::UUID(buf))
            }
            Type::Array(_) => todo!("array parsing"),
            Type::Map(_, _) => todo!("map parsing"),
            Type::Nested(_) => todo!("nested parsing"),
            Type::Tuple(_) => todo!("tuple parsing"),
            // => NULLABLE
            Type::NullableUInt8 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt8, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt8(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableUInt16 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt8, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt16(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableUInt32 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt32, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt32(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableUInt64 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt64, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt64(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableUInt128 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt128, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt128(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableUInt256 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::UInt256, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableUInt256(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt8 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int8, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt8(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt16 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int16, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt16(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt32 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int32, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt32(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt64 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int64, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt64(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt128 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int128, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt128(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableInt256 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Int256, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableInt256(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableFloat32 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Float32, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableFloat32(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableFloat64 => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self.parse(Type::Float64, reader)?.as_nullable().unwrap()),
                    0x01 => Ok(Value::NullableFloat64(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableDecimal(_, _) => {
                unimplemented!("Decimal value")
            }
            Type::NullableDecimal32(x) => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self
                        .parse(Type::Decimal32(x), reader)?
                        .as_nullable()
                        .unwrap()),
                    0x01 => Ok(Value::NullableDecimal32(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableDecimal64(x) => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self
                        .parse(Type::Decimal64(x), reader)?
                        .as_nullable()
                        .unwrap()),
                    0x01 => Ok(Value::NullableDecimal64(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            Type::NullableDecimal128(x) => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self
                        .parse(Type::Decimal128(x), reader)?
                        .as_nullable()
                        .unwrap()),
                    0x01 => Ok(Value::NullableDecimal128(None)),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
            // Type::NullableDecimal256(_) => unimplemented!("NullableDecimal256 value"),
            // Type::NullableBool => {
            //     let mut buf = [0x00_u8; 1];
            //     reader.read_exact(&mut buf)?;
            //     Ok(Value::Decimal128(i128::from_le_bytes(buf)))
            // }
            // Type::NullableString => parse_null_var!(self, reader, String, NullableString),
            // Type::NullableFixedString(_) => unimplemented!("NullableFixedString value"),
            // Type::NullableDate => parse_null_var!(self, reader, Date, NullableDate),
            // Type::NullableDate32 => parse_null_var!(self, reader, Date32, NullableDate32),
            // Type::NullableDateTime => parse_null_var!(self, reader, DateTime, NullableDateTime),
            // Type::NullableDateTime64(_) => unimplemented!("NullableDateTime64 value"),
            // Type::NullableEnum8(_) => todo!(),
            // Type::NullableEnum16(_) => todo!(),
            // Type::NullableUUID => parse_null_var!(self, reader, UUID, NullableUUID),
            _ => {
                todo!()
            }
        }
    }
}

impl RowBinFormatter {
    /// Formats a data table
    pub fn format_table(&self, table: &QueryTable) -> Vec<u8> {
        let mut buf = vec![];

        // names
        let mut n_cols = 0;
        if let Some(names) = &table.names {
            let n = names.len();
            leb128::write::unsigned(&mut buf, n as u64).unwrap();
            n_cols = n;

            for name in names {
                let mut b = self.format(&name.to_string().into());
                buf.append(&mut b);
            }
        }

        // types
        if let Some(types) = &table.types {
            debug_assert!(
                n_cols > 0,
                "table column names must be provided when types are provided"
            );
            debug_assert!(
                types.len() == n_cols,
                "mismatch between the number of types and columns"
            );
            for ty in types {
                let mut b = self.format(&ty.to_string().into());
                buf.append(&mut b);
            }
        }

        // values
        for row in &table.rows {
            for val in row {
                let mut b = self.format(val);
                buf.append(&mut b);
            }
        }

        buf
    }

    /// Parses a table with names and types
    ///
    /// This is the `RowBinaryWithNamesAndTypes` format
    pub fn parse_table_with_names_and_types(&self, bytes: &[u8]) -> Result<QueryTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        let types = self.parse_table_types_internal(&mut cursor, names.len())?;
        let rows = self.parse_table_rows_internal(&mut cursor, &types)?;

        Ok(QueryTable {
            names: Some(names),
            types: Some(types),
            rows,
        })
    }

    /// Parses a table with names (and no types)
    ///
    /// This is the `RowBinaryWithNames` format.
    /// Types must be provided to parse the rows
    pub fn parse_table_with_names(
        &self,
        bytes: &[u8],
        types: &[Type],
    ) -> Result<QueryTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        if names.len() != types.len() {
            return Err(Error(
                "Mismatch between the number of columns and types".to_string(),
            ));
        }
        let rows = self.parse_table_rows_internal(&mut cursor, types)?;

        Ok(QueryTable {
            names: Some(names),
            types: Some(types.to_vec()),
            rows,
        })
    }

    /// Parses a table without names or types
    ///
    /// This is the base `RowBinary` format
    pub fn parse_table(&self, bytes: &[u8], types: &[Type]) -> Result<QueryTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        if names.len() != types.len() {
            return Err(Error(
                "Mismatch between the number of columns and types".to_string(),
            ));
        }
        let rows = self.parse_table_rows_internal(&mut cursor, types)?;

        Ok(QueryTable {
            names: Some(names),
            types: Some(types.to_vec()),
            rows,
        })
    }

    /// Parses a table names (internal method)
    fn parse_table_names_internal(&self, cursor: &mut Cursor<&[u8]>) -> Result<Vec<String>, Error> {
        let n = leb128::read::unsigned(cursor).unwrap();
        let mut names = vec![];
        for _i in 0..n {
            let value = self.parse(Type::String, cursor)?;
            let name = match value {
                Value::String(s) => s,
                _ => unreachable!(),
            };
            names.push(name);
        }
        Ok(names)
    }

    /// Parses a table types (internal method)
    fn parse_table_types_internal(
        &self,
        cursor: &mut Cursor<&[u8]>,
        n: usize,
    ) -> Result<Vec<Type>, Error> {
        let mut types = vec![];
        for _i in 0..n {
            let value = self.parse(Type::String, cursor)?;
            let ty_str = match value {
                Value::String(s) => s,
                _ => unreachable!(),
            };
            let ty = ty_str.parse()?;
            types.push(ty);
        }
        Ok(types)
    }

    /// Parses a table data (internal method)
    fn parse_table_rows_internal(
        &self,
        cursor: &mut Cursor<&[u8]>,
        types: &[Type],
    ) -> Result<Vec<Vec<Value>>, Error> {
        // loop on all rows until everything is read
        let mut rows = vec![];
        'l_rows: loop {
            // loop on each columns
            let mut cols = vec![];
            for ty in types {
                let value = self.parse(ty.clone(), cursor)?;
                cols.push(value);
            }
            rows.push(cols);

            // break when there is nothing left to read.
            if cursor.remaining() == 0 {
                break 'l_rows;
            }
        }

        Ok(rows)
    }
}

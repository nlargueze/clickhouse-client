//! RowBinary format

#[cfg(test)]
mod tests;

use std::io::{Cursor, Read, Write};

use hyper::body::Buf;

use crate::{
    core::{DataTable, Type, Value},
    error::Error,
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

/// Formats for a nullable variant
macro_rules! format_null_var {
    ($SELF:tt, $X:tt, $VAR:tt) => {
        match $X {
            Some(x) => {
                let mut buf = vec![0x00];
                let mut v = $SELF.format(&Value::$VAR(x.clone()));
                buf.append(&mut v);
                buf
            }
            None => {
                vec![0x01]
            }
        }
    };
}

/// Parses a nullable type
macro_rules! parse_null_var {
    ($SELF:tt, $READER:tt, $TYPE:tt, $NULL_TY:tt) => {{
        let mut buf = [0x00_u8; 1];
        $READER.read_exact(&mut buf)?;
        match buf[0] {
            // 0x00 = Some
            0x00 => $SELF.parse(Type::$TYPE, $READER),
            // 0x01 = None
            0x01 => Ok(Value::$NULL_TY(None)),
            _ => Err(Error::new("invalid nullable value")),
        }
    }};
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
            Value::Array(_) => todo!("format array"),
            Value::Map(_) => todo!("format map"),
            Value::Nested(_) => todo!("format nested"),
            Value::NullableUInt8(u) => format_null_var!(self, u, UInt8),
            Value::NullableUInt16(u) => format_null_var!(self, u, UInt16),
            Value::NullableUInt32(u) => format_null_var!(self, u, UInt32),
            Value::NullableUInt64(u) => format_null_var!(self, u, UInt64),
            Value::NullableUInt128(u) => format_null_var!(self, u, UInt128),
            Value::NullableInt8(i) => format_null_var!(self, i, Int8),
            Value::NullableInt16(i) => format_null_var!(self, i, Int16),
            Value::NullableInt32(i) => format_null_var!(self, i, Int32),
            Value::NullableInt64(i) => format_null_var!(self, i, Int64),
            Value::NullableInt128(i) => format_null_var!(self, i, Int128),
            Value::NullableFloat32(f) => format_null_var!(self, f, Float32),
            Value::NullableFloat64(f) => format_null_var!(self, f, Float64),
            Value::NullableBool(b) => format_null_var!(self, b, Bool),
            Value::NullableString(s) => format_null_var!(self, s, String),
            Value::NullableFixedString(s) => format_null_var!(self, s, FixedString),
            Value::NullableDate(d) => format_null_var!(self, d, Date),
            Value::NullableDate32(d) => format_null_var!(self, d, Date32),
            Value::NullableDateTime(dt) => format_null_var!(self, dt, DateTime),
            Value::NullableDateTime64(dt) => format_null_var!(self, dt, DateTime64),
            Value::NullableUUID(id) => format_null_var!(self, id, UUID),
        }
    }

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
                unimplemented!("Decimal32 value");
            }
            Type::Decimal64(_) => {
                unimplemented!("Decimal64 value");
            }
            Type::Decimal128(_) => {
                unimplemented!("Decimal128 value");
            }
            Type::Decimal256(_) => {
                unimplemented!("Decimal256 value");
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
                let mut buf = vec![0u8; n as usize];
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
            Type::UUID => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::UUID(buf))
            }
            Type::Array(_) => todo!("array parsing"),
            Type::Map(_, _) => todo!("map parsing"),
            Type::Nested(_) => todo!("nested parsing"),
            Type::NullableUInt8 => parse_null_var!(self, reader, UInt8, NullableUInt8),
            Type::NullableUInt16 => parse_null_var!(self, reader, UInt16, NullableUInt16),
            Type::NullableUInt32 => parse_null_var!(self, reader, UInt32, NullableUInt32),
            Type::NullableUInt64 => parse_null_var!(self, reader, UInt64, NullableUInt64),
            Type::NullableUInt128 => parse_null_var!(self, reader, UInt128, NullableUInt128),
            Type::NullableUInt256 => unimplemented!("NullableUInt256 value"),
            Type::NullableInt8 => parse_null_var!(self, reader, Int8, NullableInt8),
            Type::NullableInt16 => parse_null_var!(self, reader, Int16, NullableInt16),
            Type::NullableInt32 => parse_null_var!(self, reader, Int32, NullableInt32),
            Type::NullableInt64 => parse_null_var!(self, reader, Int64, NullableInt64),
            Type::NullableInt128 => parse_null_var!(self, reader, Int128, NullableInt128),
            Type::NullableInt256 => unimplemented!("NullableInt256 value"),
            Type::NullableFloat32 => parse_null_var!(self, reader, Float32, NullableFloat32),
            Type::NullableFloat64 => parse_null_var!(self, reader, Float64, NullableFloat64),
            Type::NullableDecimal(_, _) => unimplemented!("Decimal value"),
            Type::NullableDecimal32(_) => unimplemented!("NullableDecimal32 value"),
            Type::NullableDecimal64(_) => unimplemented!("NullableDecimal64 value"),
            Type::NullableDecimal128(_) => unimplemented!("NullableDecimal128 value"),
            Type::NullableDecimal256(_) => unimplemented!("NullableDecimal256 value"),
            Type::NullableBool => parse_null_var!(self, reader, Bool, NullableBool),
            Type::NullableString => parse_null_var!(self, reader, String, NullableString),
            Type::NullableFixedString(_) => unimplemented!("NullableFixedString value"),
            Type::NullableDate => parse_null_var!(self, reader, Date, NullableDate),
            Type::NullableDate32 => parse_null_var!(self, reader, Date32, NullableDate32),
            Type::NullableDateTime => parse_null_var!(self, reader, DateTime, NullableDateTime),
            Type::NullableDateTime64(_) => unimplemented!("NullableDateTime64 value"),
            Type::NullableUUID => parse_null_var!(self, reader, UUID, NullableUUID),
        }
    }
}

impl RowBinFormatter {
    /// Formats a data table
    pub fn format_table(&self, table: &DataTable) -> Vec<u8> {
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
    pub fn parse_table_with_names_and_types(&self, bytes: &[u8]) -> Result<DataTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        let types = self.parse_table_types_internal(&mut cursor, names.len())?;
        let rows = self.parse_table_rows_internal(&mut cursor, &types)?;

        Ok(DataTable {
            names: Some(names),
            types: Some(types),
            rows,
        })
    }

    /// Parses a table with names (and no types)
    ///
    /// This is the `RowBinaryWithNames` format.
    /// Types must be provided to parse the rows
    pub fn parse_table_with_names(&self, bytes: &[u8], types: &[Type]) -> Result<DataTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        if names.len() != types.len() {
            return Err(Error(
                "Mismatch between the number of columns and types".to_string(),
            ));
        }
        let rows = self.parse_table_rows_internal(&mut cursor, types)?;

        Ok(DataTable {
            names: Some(names),
            types: Some(types.to_vec()),
            rows,
        })
    }

    /// Parses a table without names or types
    ///
    /// This is the base `RowBinary` format
    pub fn parse_table(&self, bytes: &[u8], types: &[Type]) -> Result<DataTable, Error> {
        let mut cursor = Cursor::new(bytes);

        let names = self.parse_table_names_internal(&mut cursor)?;
        if names.len() != types.len() {
            return Err(Error(
                "Mismatch between the number of columns and types".to_string(),
            ));
        }
        let rows = self.parse_table_rows_internal(&mut cursor, types)?;

        Ok(DataTable {
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

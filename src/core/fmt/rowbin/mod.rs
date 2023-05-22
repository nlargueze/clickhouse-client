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

use super::{Formatter, TableFormatter};

/// Formatter for RowBinary format
#[derive(Debug, Default)]
pub struct RowBinFormatter {
    /// With names
    with_names: bool,
    /// With types
    with_types: bool,
}

impl RowBinFormatter {
    /// Creates a new formatter without names and types
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new formatter with names and types
    pub fn new_with_names_and_types() -> Self {
        Self {
            with_names: true,
            with_types: true,
        }
    }

    /// Creates a new formatter with names
    pub fn new_with_names() -> Self {
        Self {
            with_names: true,
            with_types: false,
        }
    }

    /// With names
    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    /// With types
    pub fn with_types(mut self, with_types: bool) -> Self {
        self.with_types = with_types;
        if with_types {
            // NB: names are formatted if types are there
            self.with_names = true;
        }
        self
    }
}

impl Formatter for RowBinFormatter {
    type Target = Vec<u8>;

    type Err = Error;

    fn format(&self, value: &Value) -> Self::Target {
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
            Value::Bool(b) => {
                if *b {
                    vec![0x01]
                } else {
                    vec![0x00]
                }
            }
            Value::String(s) => {
                let mut buf = vec![];
                leb128::write::unsigned(&mut buf, s.len() as u64).unwrap();
                buf.write_all(s.as_bytes()).unwrap();
                buf
            }
            Value::FixedString(s) => s.as_bytes().to_vec(),
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
            Value::NullableUInt8(_)
            | Value::NullableUInt16(_)
            | Value::NullableUInt32(_)
            | Value::NullableUInt64(_)
            | Value::NullableUInt128(_)
            | Value::NullableUInt256(_)
            | Value::NullableInt8(_)
            | Value::NullableInt16(_)
            | Value::NullableInt32(_)
            | Value::NullableInt64(_)
            | Value::NullableInt128(_)
            | Value::NullableInt256(_)
            | Value::NullableFloat32(_)
            | Value::NullableFloat64(_)
            | Value::NullableDecimal32(_)
            | Value::NullableDecimal64(_)
            | Value::NullableDecimal128(_)
            | Value::NullableBool(_)
            | Value::NullableString(_)
            | Value::NullableFixedString(_)
            | Value::NullableDate(_)
            | Value::NullableDate32(_)
            | Value::NullableDateTime(_)
            | Value::NullableDateTime64(_)
            | Value::NullableUUID(_)
            | Value::NullableEnum8(_)
            | Value::NullableEnum16(_) => {
                if let Some(v) = value.as_non_nullable() {
                    let mut buf = vec![0x00];
                    let mut v_fmt = self.format(&v);
                    buf.append(&mut v_fmt);
                    buf
                } else {
                    vec![0x01]
                }
            }
        }
    }

    /// Parses a [Value] from a reader and a type
    fn parse(&self, reader: &mut impl Read, ty: Type) -> Result<Value, Self::Err> {
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
            Type::UUID => {
                let mut buf = [0x00_u8; 16];
                reader.read_exact(&mut buf)?;
                Ok(Value::UUID(buf))
            }
            Type::Enum(_) => todo!("enum parsing"),
            Type::Enum8(_) => todo!("enum parsing"),
            Type::Enum16(_) => todo!("enum parsing"),
            Type::Array(_) => todo!("array parsing"),
            Type::Map(_, _) => todo!("map parsing"),
            Type::Nested(_) => todo!("nested parsing"),
            Type::Tuple(_) => todo!("tuple parsing"),
            Type::NullableUInt8
            | Type::NullableUInt16
            | Type::NullableUInt32
            | Type::NullableUInt64
            | Type::NullableUInt128
            | Type::NullableUInt256
            | Type::NullableInt8
            | Type::NullableInt16
            | Type::NullableInt32
            | Type::NullableInt64
            | Type::NullableInt128
            | Type::NullableInt256
            | Type::NullableFloat32
            | Type::NullableFloat64
            | Type::NullableDecimal(_, _)
            | Type::NullableDecimal32(_)
            | Type::NullableDecimal64(_)
            | Type::NullableDecimal128(_)
            | Type::NullableDecimal256(_)
            | Type::NullableBool
            | Type::NullableString
            | Type::NullableFixedString(_)
            | Type::NullableDate
            | Type::NullableDate32
            | Type::NullableDateTime
            | Type::NullableDateTime64(_)
            | Type::NullableUUID
            | Type::NullableEnum(_)
            | Type::NullableEnum8(_)
            | Type::NullableEnum16(_) => {
                let mut buf = [0x00_u8; 1];
                reader.read_exact(&mut buf)?;
                match buf[0] {
                    0x00 => Ok(self
                        .parse(reader, ty.as_non_nullable())?
                        .as_nullable()
                        .unwrap()),
                    0x01 => Ok(Value::null(ty).unwrap()),
                    _ => Err(Error::new("invalid nullable value")),
                }
            }
        }
    }
}

impl TableFormatter for RowBinFormatter {
    fn format_table(&self, table: &QueryTable) -> Self::Target {
        let mut buf = vec![];

        // Column names
        if self.with_names {
            let n = table.names.len();
            leb128::write::unsigned(&mut buf, n as u64).unwrap();
            for name in &table.names {
                let mut b = self.format(&name.to_string().into());
                buf.append(&mut b);
            }
        }

        // Column types
        if self.with_types {
            debug_assert!(
                table.types.len() == table.names.len(),
                "mismatch between the number of column names and types"
            );
            for ty in &table.types {
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

    fn parse_table(
        &self,
        reader: &mut impl Read,
        types: Option<&[&Type]>,
    ) -> Result<QueryTable, Self::Err> {
        // convert to cursor
        let mut bytes = vec![];
        reader.read_to_end(&mut bytes)?;
        let mut cursor = Cursor::new(bytes);

        // column names
        let mut names = vec![];
        if self.with_names {
            let n = leb128::read::unsigned(&mut cursor).unwrap();
            for _i in 0..n {
                let value = self.parse(&mut cursor, Type::String)?;
                let name = match value {
                    Value::String(s) => s,
                    _ => unreachable!(),
                };
                names.push(name);
            }
        }

        // column types
        let n = names.len();
        let mut types_read = vec![];
        if self.with_types {
            for _i in 0..n {
                let value = self.parse(&mut cursor, Type::String)?;
                let ty_str = match value {
                    Value::String(s) => s,
                    _ => unreachable!(),
                };
                let ty: Type = ty_str.parse()?;
                types_read.push(ty);
            }
        }
        let types = if !types_read.is_empty() {
            types_read
        } else {
            match types {
                Some(list) => list.iter().map(|ty| (*ty).clone()).collect(),
                None => {
                    return Err(Error(
                        "table types are missing, and must be provided to parse the values"
                            .to_string(),
                    ));
                }
            }
        };

        // values
        let mut rows = vec![];
        'l_rows: loop {
            // loop on each columns
            let mut cols = vec![];
            for ty in &types {
                let value = self.parse(&mut cursor, ty.clone())?;
                cols.push(value);
            }
            rows.push(cols);

            // break when there is nothing left to read.
            if cursor.remaining() == 0 {
                break 'l_rows;
            }
        }

        Ok(QueryTable { names, types, rows })
    }
}

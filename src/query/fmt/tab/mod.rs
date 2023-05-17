//! TabSeparated format

#[cfg(test)]
mod tests;

use std::{collections::HashMap, str::FromStr};

use ethnum::{I256, U256};
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::{
    error::Error,
    query::QueryData,
    value::{
        time::{DateExt, DateTimeExt},
        Type, Value,
    },
};

use super::Formatter;

/// TabSeparated formatter
#[derive(Debug, Default)]
pub struct TsvFormatter {
    /// Raw
    raw: bool,
    /// With column names
    with_names: bool,
    /// With column types
    with_types: bool,
}

impl TsvFormatter {
    /// Use the default variant
    pub fn new() -> Self {
        Self::default()
    }

    /// Use the variant with names
    pub fn with_names() -> Self {
        Self {
            raw: false,
            with_names: true,
            with_types: false,
        }
    }

    /// Use the variant with names and types
    pub fn with_names_and_types() -> Self {
        Self {
            raw: false,
            with_names: true,
            with_types: true,
        }
    }

    /// Use the raw variant
    pub fn raw() -> Self {
        Self {
            raw: true,
            with_names: false,
            with_types: false,
        }
    }

    /// Use the raw variant with names
    pub fn raw_with_names() -> Self {
        Self {
            raw: true,
            with_names: true,
            with_types: false,
        }
    }

    /// Use the raw variant with names and types
    pub fn raw_with_names_and_types() -> Self {
        Self {
            raw: true,
            with_names: true,
            with_types: true,
        }
    }
}

impl Formatter for TsvFormatter {
    fn serialize_value(&self, value: Value) -> Vec<u8> {
        self.format_value(value).into_bytes()
    }

    fn serialize_query_data(&self, data: QueryData) -> Result<Vec<u8>, Error> {
        self.format_data(data).map(|s| s.into_bytes())
    }

    fn deserialize_value(&self, bytes: &[u8], ty: Type) -> Result<Value, Error> {
        let value = String::from_utf8(bytes.to_vec())?;
        self.parse_value(&value, ty)
    }

    fn deserialize_query_data(
        &self,
        bytes: &[u8],
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<QueryData, Error> {
        let value = String::from_utf8(bytes.to_vec())?;
        self.parse_data(&value, mapping)
    }
}

/// NULL value
const NULL: &str = r"\N";

impl TsvFormatter {
    /// Formats a [Value]
    pub fn format_value(&self, value: Value) -> String {
        self.format_value_iter(value, false)
    }

    /// Formats a [Value] recursively
    fn format_value_iter(&self, value: Value, is_within_array: bool) -> String {
        /// Implements the nullable variant for formatting
        macro_rules! impl_nullable {
            ($VAL:tt, $VAR:ident) => {
                match $VAL {
                    Some(v) => self.format_value_iter(Value::$VAR(v), is_within_array),
                    None => NULL.to_string(),
                }
            };
        }

        match value {
            Value::UInt8(v) => v.to_string(),
            Value::UInt16(v) => v.to_string(),
            Value::UInt32(v) => v.to_string(),
            Value::UInt64(v) => v.to_string(),
            Value::UInt128(v) => v.to_string(),
            Value::UInt256(_) => {
                let u256: U256 = value.try_into().unwrap();
                u256.to_string()
            }
            Value::Int8(v) => v.to_string(),
            Value::Int16(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Int64(v) => v.to_string(),
            Value::Int128(v) => v.to_string(),
            Value::Int256(_) => {
                let i256: I256 = value.try_into().unwrap();
                i256.to_string()
            }
            Value::Float32(v) => v.to_string(),
            Value::Float64(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            Value::String(v) => {
                let s = if self.raw { v } else { v.escape() };
                if is_within_array {
                    s.enclose()
                } else {
                    s
                }
            }
            Value::UUID(_) => {
                let uuid: Uuid = value.try_into().unwrap();
                uuid.as_hyphenated().to_string()
            }
            Value::Date(_) | Value::Date32(_) => {
                let date: Date = value.try_into().unwrap();
                let s = date.format_yyyy_mm_dd();
                if is_within_array {
                    s.enclose()
                } else {
                    s
                }
            }
            Value::DateTime(_) => {
                let dt: OffsetDateTime = value.try_into().unwrap();
                let s = dt.format_yyyy_mm_dd_hh_mm_ss();
                if is_within_array {
                    s.enclose()
                } else {
                    s
                }
            }
            Value::DateTime64(_) => {
                let dt: OffsetDateTime = value.try_into().unwrap();
                let s = dt.format_yyyy_mm_dd_hh_mm_ss_ns();
                if is_within_array {
                    s.enclose()
                } else {
                    s
                }
            }
            Value::Enum8(v) => v.to_string(),
            Value::Enum16(v) => v.to_string(),
            Value::Array(v) => {
                format!(
                    "[{}]",
                    v.into_iter()
                        .map(|v| self.format_value_iter(v, true))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Tuple(v) => {
                format!(
                    "({})",
                    v.into_iter()
                        .map(|v| self.format_value_iter(v, true))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Map(v) => {
                let mut kv = v
                    .into_iter()
                    .map(|(k, v)| format!("'{}': {}", k, self.format_value_iter(v, true)))
                    .collect::<Vec<_>>();
                kv.sort();
                format!("{{{}}}", kv.join(", "))
            }
            Value::Nested(fields) => {
                let values = fields.into_values().collect::<Vec<_>>();
                let value = Value::Array(values);
                self.format_value_iter(value, false)
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

    /// Formats a [QueryTable]
    pub fn format_data(&self, data: QueryData) -> Result<String, Error> {
        // each value is followed by a tab (except the last value in a row)
        // the last value is followed by a newline (inc. last row)
        let mut buf = String::new();
        let parts = data.into_parts();
        if self.with_names {
            if let Some(names) = parts.names {
                let row = self.format_table_row(names)?;
                buf.push_str(row.as_str());
            } else {
                return Err(Error::new("Table is missing the column names"));
            }
        }

        if self.with_types {
            if let Some(types) = parts.types {
                let types = types.into_iter().map(|t| t.to_string()).collect();
                let row = self.format_table_row(types)?;
                buf.push_str(row.as_str());
            } else {
                return Err(Error::new("Table is missing the column types"));
            }
        }

        for row in parts.rows {
            let values = row
                .into_iter()
                .map(|value| self.format_value(value))
                .collect::<Vec<_>>();
            let row = self.format_table_row(values)?;
            buf.push_str(row.as_str());
        }

        Ok(buf)
    }

    /// Formats a table row
    fn format_table_row(&self, values: Vec<String>) -> Result<String, Error> {
        let mut buf = String::new();
        let n = values.len();
        for (i, value) in values.iter().enumerate() {
            buf.push_str(value);
            if i < n - 1 {
                buf.push('\t');
            } else {
                buf.push('\n');
            }
        }
        Ok(buf)
    }

    /// Parses a [Value]
    fn parse_value(&self, value: &str, ty: Type) -> Result<Value, Error> {
        self.parse_value_iter(value, ty, false)
    }

    /// Parses a [Value] recursively
    #[allow(clippy::only_used_in_recursion)]
    fn parse_value_iter(
        &self,
        value: &str,
        ty: Type,
        is_within_array: bool,
    ) -> Result<Value, Error> {
        match ty {
            Type::UInt8 => {
                let v = value.parse::<u8>()?;
                Ok(v.into())
            }
            Type::UInt16 => {
                let v = value.parse::<u16>()?;
                Ok(v.into())
            }
            Type::UInt32 => {
                let v = value.parse::<u32>()?;
                Ok(v.into())
            }
            Type::UInt64 => {
                let v = value.parse::<u64>()?;
                Ok(v.into())
            }
            Type::UInt128 => {
                let v = value.parse::<u128>()?;
                Ok(v.into())
            }
            Type::UInt256 => {
                let v = value.parse::<U256>()?;
                Ok(v.into())
            }
            Type::Int8 => {
                let v = value.parse::<i8>()?;
                Ok(v.into())
            }
            Type::Int16 => {
                let v = value.parse::<i16>()?;
                Ok(v.into())
            }
            Type::Int32 => {
                let v = value.parse::<i32>()?;
                Ok(v.into())
            }
            Type::Int64 => {
                let v = value.parse::<i64>()?;
                Ok(v.into())
            }
            Type::Int128 => {
                let v = value.parse::<i128>()?;
                Ok(v.into())
            }
            Type::Int256 => {
                let v: I256 = value.parse::<I256>()?;
                Ok(v.into())
            }
            Type::Float32 => {
                let v = value.parse::<f32>()?;
                Ok(v.into())
            }
            Type::Float64 => {
                let v = value.parse::<f64>()?;
                Ok(v.into())
            }
            Type::Decimal(_, _)
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_) => {
                let v = value.parse::<f64>()?;
                Ok(v.into())
            }
            Type::Bool => {
                let v = value.parse::<bool>()?;
                Ok(v.into())
            }
            Type::String => {
                let v = value.unescape();
                let v = if is_within_array { v.unenclose() } else { v };
                Ok(v.into())
            }
            Type::FixedString(_) => {
                let v = value.unescape();
                let v = if is_within_array { v.unenclose() } else { v };
                Ok(v.into())
            }
            Type::UUID => {
                let v = value.parse::<Uuid>()?;
                Ok(v.into())
            }
            Type::Date | Type::Date32 => {
                let v = value.to_string();
                let v = if is_within_array { v.unenclose() } else { v };
                let date = Date::parse_yyyy_mm_dd(&v)?;
                Ok(date.into())
            }
            Type::DateTime => {
                let v = value.to_string();
                let v = if is_within_array { v.unenclose() } else { v };
                let dt = OffsetDateTime::parse_yyyy_mm_dd_hh_mm_ss(&v)?;
                Ok(dt.into())
            }
            Type::DateTime64(_) => {
                let v = value.to_string();
                let v = if is_within_array { v.unenclose() } else { v };
                let dt = OffsetDateTime::parse_yyyy_mm_dd_hh_mm_ss_ns(&v)?;
                Ok(dt.into())
            }
            Type::Enum8(variants) => match variants.get(value) {
                Some(i) => Ok(Value::Enum8(*i)),
                None => Err(Error::new(
                    format!("Invalid enum variant: {value}").as_str(),
                )),
            },
            Type::Enum16(variants) => match variants.get(value) {
                Some(i) => Ok(Value::Enum16(*i)),
                None => Err(Error::new(
                    format!("Invalid enum variant: {value}").as_str(),
                )),
            },
            Type::Array(ty) => {
                if let Some(s) = value.trim().strip_prefix('[') {
                    if let Some(s) = s.strip_suffix(']') {
                        let parts = s.trim().split(',').collect::<Vec<_>>();
                        let mut values = vec![];
                        for part in parts {
                            let value = self.parse_value_iter(part.trim(), *ty.clone(), true)?;
                            values.push(value);
                        }
                        Ok(Value::Array(values))
                    } else {
                        Err(Error::new("Invalid array"))
                    }
                } else {
                    Err(Error::new("Invalid array"))
                }
            }
            Type::Tuple(types) => {
                if let Some(s) = value.trim().strip_prefix('(') {
                    if let Some(s) = s.strip_suffix(')') {
                        let parts = s.trim().split(',').collect::<Vec<_>>();
                        if parts.len() != types.len() {
                            return Err(Error::new("Invalid tuple"));
                        }
                        let mut values = vec![];
                        for (i, part) in parts.into_iter().enumerate() {
                            let value =
                                self.parse_value_iter(part.trim(), types[i].clone(), true)?;
                            values.push(value);
                        }
                        Ok(Value::Tuple(values))
                    } else {
                        Err(Error::new("Invalid tuple"))
                    }
                } else {
                    Err(Error::new("Invalid tuple"))
                }
            }
            Type::Map(_ty_key, ty_val) => {
                if let Some(s) = value.trim().strip_prefix('{') {
                    if let Some(s) = s.strip_suffix('}') {
                        let mut map = HashMap::new();
                        let kv_pairs = s.trim().split(',').collect::<Vec<_>>();
                        for kv in kv_pairs {
                            let parts = kv.trim().split(':').collect::<Vec<_>>();
                            if parts.len() != 2 {
                                return Err(Error::new("Invalid map"));
                            }
                            let key = parts[0].trim().unenclose();
                            let value_str = parts[1].trim();
                            let value = self.parse_value_iter(value_str, *ty_val.clone(), true)?;
                            map.insert(key, value);
                        }
                        Ok(Value::Map(map))
                    } else {
                        Err(Error::new("Invalid map"))
                    }
                } else {
                    Err(Error::new("Invalid map"))
                }
            }
            Type::Nested(fields) => {
                if let Some(s) = value.trim().strip_prefix('[') {
                    if let Some(s) = s.strip_suffix(']') {
                        let parts = s.trim().split(',').collect::<Vec<_>>();
                        let mut map = HashMap::new();
                        for (i, part) in parts.into_iter().enumerate() {
                            let (key, ty) =
                                fields.get(i).ok_or(Error::new("Invalid nested value"))?;
                            let value = self.parse_value_iter(part.trim(), ty.clone(), true)?;
                            map.insert(key.to_string(), value);
                        }
                        Ok(Value::Nested(map))
                    } else {
                        Err(Error::new("Invalid array"))
                    }
                } else {
                    Err(Error::new("Invalid array"))
                }
            }
            Type::NullableUInt8 => match value {
                NULL => Ok(Value::NullableUInt8(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt8, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUInt16 => match value {
                NULL => Ok(Value::NullableUInt16(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt16, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUInt32 => match value {
                NULL => Ok(Value::NullableUInt32(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt32, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUInt64 => match value {
                NULL => Ok(Value::NullableUInt64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt64, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUInt128 => match value {
                NULL => Ok(Value::NullableUInt128(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt128, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUInt256 => match value {
                NULL => Ok(Value::NullableUInt256(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UInt256, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt8 => match value {
                NULL => Ok(Value::NullableInt8(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int8, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt16 => match value {
                NULL => Ok(Value::NullableInt16(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int16, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt32 => match value {
                NULL => Ok(Value::NullableInt32(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int32, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt64 => match value {
                NULL => Ok(Value::NullableInt64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int64, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt128 => match value {
                NULL => Ok(Value::NullableInt128(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int128, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableInt256 => match value {
                NULL => Ok(Value::NullableInt256(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Int256, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableFloat32 => match value {
                NULL => Ok(Value::NullableFloat32(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Float32, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableFloat64 => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Float64, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDecimal(p, s) => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Decimal(p, s), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDecimal32(s) => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Decimal32(s), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDecimal64(s) => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Decimal64(s), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDecimal128(s) => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Decimal128(s), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDecimal256(s) => match value {
                NULL => Ok(Value::NullableFloat64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Decimal256(s), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableBool => match value {
                NULL => Ok(Value::NullableBool(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Bool, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableString => match value {
                NULL => Ok(Value::NullableString(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::String, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableFixedString(n) => match value {
                NULL => Ok(Value::NullableString(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::FixedString(n), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableUUID => match value {
                NULL => Ok(Value::NullableUUID(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::UUID, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDate => match value {
                NULL => Ok(Value::NullableDate(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Date, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDate32 => match value {
                NULL => Ok(Value::NullableDate32(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Date32, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDateTime => match value {
                NULL => Ok(Value::NullableDateTime(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::DateTime, false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableDateTime64(p) => match value {
                NULL => Ok(Value::NullableDateTime64(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::DateTime64(p), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableEnum8(variants) => match value {
                NULL => Ok(Value::NullableEnum8(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Enum8(variants.clone()), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
            Type::NullableEnum16(variants) => match value {
                NULL => Ok(Value::NullableEnum16(None)),
                _ => {
                    let v = self.parse_value_iter(value, Type::Enum16(variants.clone()), false)?;
                    Ok(v.into_nullable().unwrap())
                }
            },
        }
    }

    /// Parses a [QueryTable]
    pub fn parse_data(
        &self,
        value: &str,
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<QueryData, Error> {
        // split rows
        let mut rows = value.split('\n').collect::<Vec<_>>();

        // parse names and types from the buffer
        let mut data = if self.with_names {
            if rows.is_empty() {
                return Err(Error::new("Table is missing the row with names"));
            }
            let row = rows.remove(0);
            let names = row.split('\t').collect::<Vec<_>>();

            if self.with_types {
                if rows.is_empty() {
                    return Err(Error::new("Table is missing the row with types"));
                }
                let row = rows.remove(0);
                let types = row
                    .split('\t')
                    .map(Type::from_str)
                    .collect::<Result<Vec<Type>, Error>>()?;
                let names_and_types = names
                    .into_iter()
                    .enumerate()
                    .map(|(i, n)| (n, types[i].clone()))
                    .collect();

                QueryData::with_names_and_types(names_and_types)
            } else {
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

        for row_str in rows {
            if row_str.is_empty() {
                break;
            }

            let mut row = vec![];
            for (i, value) in row_str.split('\t').enumerate() {
                let ty = types
                    .get(i)
                    .ok_or(Error(format!("No type for value at index {i}")))?
                    .clone();
                row.push(self.parse_value(value, ty)?);
            }
            data.add_row(row);
        }

        Ok(data)
    }
}

/// Extension trait for strings
trait StringExt {
    /// Escapes a string
    fn escape(&self) -> String;

    /// Unescapes a string
    fn unescape(&self) -> String;

    /// Encloses a string
    fn enclose(&self) -> String;

    /// Unencloses a string
    fn unenclose(&self) -> String;
}

impl StringExt for &str {
    fn escape(&self) -> String {
        self.replace('\\', r"\b")
            .replace(' ', r"\n")
            .replace('\t', r"\t")
            .replace('\'', r"\'")
    }

    fn unescape(&self) -> String {
        self.replace(r"\b", "\\")
            .replace(r"\n", " ")
            .replace(r"\t", "\t")
            .replace(r"\'", "\'")
    }

    fn enclose(&self) -> String {
        format!("'{self}'")
    }

    fn unenclose(&self) -> String {
        self.trim_start_matches('\'')
            .trim_end_matches('\'')
            .to_string()
    }
}

impl StringExt for String {
    fn escape(&self) -> String {
        self.as_str().escape()
    }

    fn unescape(&self) -> String {
        self.as_str().unescape()
    }

    fn enclose(&self) -> String {
        self.as_str().enclose()
    }

    fn unenclose(&self) -> String {
        self.as_str().unenclose()
    }
}

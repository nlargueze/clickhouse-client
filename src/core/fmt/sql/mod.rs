//! SQL format

#[cfg(test)]
mod tests;

use std::io::Read;

use crate::{
    core::{Type, Value},
    error::Error,
};

use super::Formatter;

/// Formatter for SQL strings
#[derive(Debug, Default)]
pub struct SqlFormatter;

impl SqlFormatter {
    /// Creates a new formatter
    pub fn new() -> Self {
        Self::default()
    }
}

impl Formatter for SqlFormatter {
    type Target = String;
    type Err = Error;

    fn format(&self, value: &Value) -> Self::Target {
        match value {
            Value::UInt8(u) => u.to_string(),
            Value::UInt16(u) => u.to_string(),
            Value::UInt32(u) => u.to_string(),
            Value::UInt64(u) => u.to_string(),
            Value::UInt128(u) => u.to_string(),
            Value::UInt256(_u) => todo!("UInt256 sql formatting"),
            Value::Int8(i) => i.to_string(),
            Value::Int16(i) => i.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Int128(i) => i.to_string(),
            Value::Int256(_i) => todo!("Int256 sql formatting"),
            Value::Float32(f) => f.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Decimal32(d) => d.to_string(),
            Value::Decimal64(d) => d.to_string(),
            Value::Decimal128(d) => d.to_string(),
            Value::Bool(b) => match b {
                false => "0".to_string(),
                true => "1".to_string(),
            },
            Value::String(s) => format!("'{s}'"),
            Value::FixedString(s) => format!("'{s}'"),
            Value::Date(d) => {
                #[cfg(feature = "time")]
                {
                    use crate::core::time::FORMAT_DATE;
                    use ::time::{Date, Duration, Month};
                    let date = Date::from_calendar_date(1970, Month::January, 1).unwrap()
                        + Duration::days((*d).into());
                    let date_str = date.format(FORMAT_DATE).expect("invalid date");
                    format!("'{date_str}'")
                }
                #[cfg(not(feature = "time"))]
                {
                    unimplemented!("Date formatting requires the time feature");
                }
            }
            Value::Date32(d) => {
                #[cfg(feature = "time")]
                {
                    use crate::core::time::FORMAT_DATE;
                    use ::time::{Date, Duration, Month};
                    let date = Date::from_calendar_date(1970, Month::January, 1).unwrap()
                        + Duration::days((*d).into());
                    let date_str = date.format(FORMAT_DATE).expect("invalid date");
                    format!("'{date_str}'")
                }
                #[cfg(not(feature = "time"))]
                {
                    unimplemented!("Date formatting requires the time feature");
                }
            }
            Value::DateTime(dt) => {
                #[cfg(feature = "time")]
                {
                    use crate::core::time::FORMAT_DATETIME;
                    use ::time::OffsetDateTime;
                    let date = OffsetDateTime::from_unix_timestamp((*dt).into()).unwrap();
                    let dt_str = date.format(FORMAT_DATETIME).expect("invalid date");
                    format!("'{dt_str}'")
                }
                #[cfg(not(feature = "time"))]
                {
                    unimplemented!("Date formatting requires the time feature");
                }
            }
            Value::DateTime64(dt) => {
                #[cfg(feature = "time")]
                {
                    use crate::core::time::FORMAT_DATETIME64;
                    use ::time::OffsetDateTime;
                    let date = OffsetDateTime::from_unix_timestamp_nanos((*dt).into()).unwrap();
                    let dt_str = date.format(FORMAT_DATETIME64).expect("invalid date");
                    format!("'{dt_str}'")
                }
                #[cfg(not(feature = "time"))]
                {
                    unimplemented!("Date formatting requires the time feature");
                }
            }
            Value::UUID(id) => {
                #[cfg(feature = "uuid")]
                {
                    use ::uuid::Uuid;
                    let uuid_str = Uuid::from_bytes(*id).to_string();
                    format!("'{uuid_str}'")
                }
                #[cfg(not(feature = "uuid"))]
                {
                    unimplemented!("UUI formatting requires the uui feature");
                }
            }
            Value::Enum8(i) => i.to_string(),
            Value::Enum16(i) => i.to_string(),
            Value::Array(arr) => {
                let values = arr
                    .iter()
                    .map(|elt| self.format(elt))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{values}]")
            }
            Value::Map(map) => {
                let kvs = map
                    .iter()
                    .map(|(k, v)| {
                        let v = self.format(v);
                        format!("'{k}': {v}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{kvs}}}")
            }
            Value::Nested(nested) => self.format(&Value::Map(nested.clone())),
            // Nullable values
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
                    self.format(&v)
                } else {
                    "NULL".to_string()
                }
            }
        }
    }

    fn parse(&self, _reader: &mut impl Read, _ty: Type) -> Result<Value, Self::Err> {
        unimplemented!("SQL parsing")
    }
}

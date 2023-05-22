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
    type Ok = String;
    type Err = Error;

    fn format(&self, value: &Value) -> Self::Ok {
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
            Value::NullableUInt8(u) => match u {
                Some(u) => self.format(&Value::UInt8(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableUInt16(u) => match u {
                Some(u) => self.format(&Value::UInt16(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableUInt32(u) => match u {
                Some(u) => self.format(&Value::UInt32(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableUInt64(u) => match u {
                Some(u) => self.format(&Value::UInt64(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableUInt128(u) => match u {
                Some(u) => self.format(&Value::UInt128(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableUInt256(u) => match u {
                Some(u) => self.format(&Value::UInt256(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableInt8(i) => match i {
                Some(i) => self.format(&Value::Int8(*i)),
                None => "NULL".to_string(),
            },
            Value::NullableInt16(i) => match i {
                Some(i) => self.format(&Value::Int16(*i)),
                None => "NULL".to_string(),
            },
            Value::NullableInt32(i) => match i {
                Some(i) => self.format(&Value::Int32(*i)),
                None => "NULL".to_string(),
            },
            Value::NullableInt64(i) => match i {
                Some(i) => self.format(&Value::Int64(*i)),
                None => "NULL".to_string(),
            },
            Value::NullableInt128(i) => match i {
                Some(i) => self.format(&Value::Int128(*i)),
                None => "NULL".to_string(),
            },
            Value::NullableInt256(u) => match u {
                Some(u) => self.format(&Value::Int256(*u)),
                None => "NULL".to_string(),
            },
            Value::NullableFloat32(f) => match f {
                Some(f) => self.format(&Value::Float32(*f)),
                None => "NULL".to_string(),
            },
            Value::NullableFloat64(f) => match f {
                Some(f) => self.format(&Value::Float64(*f)),
                None => "NULL".to_string(),
            },
            Value::NullableDecimal32(d) => match d {
                Some(d) => self.format(&Value::Decimal32(*d)),
                None => "NULL".to_string(),
            },
            Value::NullableDecimal64(d) => match d {
                Some(d) => self.format(&Value::Decimal64(*d)),
                None => "NULL".to_string(),
            },
            Value::NullableDecimal128(d) => match d {
                Some(d) => self.format(&Value::Decimal128(*d)),
                None => "NULL".to_string(),
            },
            Value::NullableBool(b) => match b {
                Some(b) => self.format(&Value::Bool(*b)),
                None => "NULL".to_string(),
            },
            Value::NullableString(s) => match s {
                Some(s) => self.format(&Value::String(s.to_string())),
                None => "NULL".to_string(),
            },
            Value::NullableFixedString(s) => match s {
                Some(s) => self.format(&Value::FixedString(s.to_string())),
                None => "NULL".to_string(),
            },
            Value::NullableDate(d) => match d {
                Some(d) => self.format(&Value::Date(*d)),
                None => "NULL".to_string(),
            },
            Value::NullableDate32(d) => match d {
                Some(d) => self.format(&Value::Date32(*d)),
                None => "NULL".to_string(),
            },
            Value::NullableDateTime(dt) => match dt {
                Some(dt) => self.format(&Value::DateTime(*dt)),
                None => "NULL".to_string(),
            },
            Value::NullableDateTime64(dt) => match dt {
                Some(dt) => self.format(&Value::DateTime64(*dt)),
                None => "NULL".to_string(),
            },
            Value::NullableUUID(id) => match id {
                Some(id) => self.format(&Value::UUID(*id)),
                None => "NULL".to_string(),
            },
        }
    }

    fn parse(&self, _ty: Type, _reader: &mut impl Read) -> Result<Value, Self::Err> {
        unimplemented!("SQL parsing")
    }
}

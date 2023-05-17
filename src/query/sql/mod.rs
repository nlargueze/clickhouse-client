//! SQL

#[cfg(test)]
mod tests;

use ethnum::{I256, U256};
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::value::{
    time::{DateExt, DateTimeExt},
    Value,
};

impl Value {
    /// Converts a [Value] to a SQL string
    pub fn to_sql_string(&self) -> String {
        /// Implements the nullable variant for formatting
        macro_rules! impl_nullable {
            ($VAL:expr, $VAR:ident) => {
                match $VAL {
                    Some(v) => Value::$VAR(v).to_sql_string(),
                    None => "NULL".to_string(),
                }
            };
        }

        match self {
            Value::UInt8(v) => v.to_string(),
            Value::UInt16(v) => v.to_string(),
            Value::UInt32(v) => v.to_string(),
            Value::UInt64(v) => v.to_string(),
            Value::UInt128(v) => v.to_string(),
            Value::UInt256(_) => {
                let u256 = self.clone().try_into::<U256>().unwrap();
                u256.to_string()
            }
            Value::Int8(v) => v.to_string(),
            Value::Int16(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Int64(v) => v.to_string(),
            Value::Int128(v) => v.to_string(),
            Value::Int256(_) => {
                let i256 = self.clone().try_into::<I256>().unwrap();
                i256.to_string()
            }
            Value::Float32(v) => v.to_string(),
            Value::Float64(v) => v.to_string(),
            Value::Bool(v) => match v {
                false => "0".to_string(),
                true => "1".to_string(),
            },
            Value::String(v) => format!("'{}'", v),
            Value::UUID(_) => {
                // UUID string uses a 8-4-4-4-12 representation
                let uuid = self.clone().try_into::<Uuid>().unwrap();
                let uuid_str = uuid.as_hyphenated().to_string();
                format!("'{uuid_str}'")
            }
            Value::Date(_) | Value::Date32(_) => {
                let date = self.clone().try_into::<Date>().unwrap();
                let date_str = date.format_yyyy_mm_dd();
                format!("'{date_str}'")
            }
            Value::DateTime(_) => {
                let dt = self.clone().try_into::<OffsetDateTime>().unwrap();
                let dt_str = dt.format_yyyy_mm_dd_hh_mm_ss();
                format!("'{dt_str}'")
            }
            Value::DateTime64(_) => {
                let dt = self.clone().try_into::<OffsetDateTime>().unwrap();
                let dt_str = dt.format_yyyy_mm_dd_hh_mm_ss_ns();
                format!("'{dt_str}'")
            }
            Value::Enum8(v) => v.to_string(),
            Value::Enum16(v) => v.to_string(),
            Value::Array(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|v| v.to_sql_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Tuple(values) => format!(
                "({})",
                values
                    .iter()
                    .map(|v| v.to_sql_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Map(values) => format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|(k, v)| format!("'{}': {}", k, v.to_sql_string()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Nested(values) => format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|(k, v)| format!("'{}': {}", k, v.to_sql_string()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::NullableUInt8(v) => impl_nullable!(*v, UInt8),
            Value::NullableUInt16(v) => impl_nullable!(*v, UInt16),
            Value::NullableUInt32(v) => impl_nullable!(*v, UInt32),
            Value::NullableUInt64(v) => impl_nullable!(*v, UInt64),
            Value::NullableUInt128(v) => impl_nullable!(*v, UInt128),
            Value::NullableUInt256(v) => impl_nullable!(*v, UInt256),
            Value::NullableInt8(v) => impl_nullable!(*v, Int8),
            Value::NullableInt16(v) => impl_nullable!(*v, Int16),
            Value::NullableInt32(v) => impl_nullable!(*v, Int32),
            Value::NullableInt64(v) => impl_nullable!(*v, Int64),
            Value::NullableInt128(v) => impl_nullable!(*v, Int128),
            Value::NullableInt256(v) => impl_nullable!(*v, Int256),
            Value::NullableFloat32(v) => impl_nullable!(*v, Float32),
            Value::NullableFloat64(v) => impl_nullable!(*v, Float64),
            Value::NullableBool(v) => impl_nullable!(*v, Bool),
            Value::NullableString(v) => impl_nullable!(v.clone(), String),
            Value::NullableUUID(v) => impl_nullable!(*v, UUID),
            Value::NullableDate(v) => impl_nullable!(*v, Date),
            Value::NullableDate32(v) => impl_nullable!(*v, Date32),
            Value::NullableDateTime(v) => impl_nullable!(*v, DateTime),
            Value::NullableDateTime64(v) => impl_nullable!(*v, DateTime64),
            Value::NullableEnum8(v) => impl_nullable!(*v, Enum8),
            Value::NullableEnum16(v) => impl_nullable!(*v, Enum16),
        }
    }
}

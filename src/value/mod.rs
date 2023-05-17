//! Data types

mod core;
mod ext;
mod ty;

#[cfg(test)]
mod tests;

pub use core::*;
pub use ext::*;
pub use ty::*;

use std::collections::HashMap;

use ::time::{Date, OffsetDateTime};
use ::uuid::Uuid;
use ethnum::{I256, U256};

use crate::error::Error;
use ext::time::{DateExt, DateTimeExt};

/// Trait to represent a Clickhouse value
pub trait ChValue: Sized {
    /// Returns the Clickhouse type
    fn ch_type() -> Type;

    /// Returns the Clickhouse value
    fn into_ch_value(self) -> Value;

    /// Parses from a [Value]
    fn from_ch_value(value: Value) -> Result<Self, Error>;

    /// Converts to an SQL value
    fn into_sql(self) -> String {
        self.into_ch_value().to_sql_string()
    }
}

// derive the Into<Value> implementation
impl<T> From<T> for Value
where
    T: ChValue,
{
    fn from(value: T) -> Self {
        value.into_ch_value()
    }
}

/// Clickhouse field value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// u8
    UInt8(u8),
    /// u16
    UInt16(u16),
    /// u32
    UInt32(u32),
    /// u64
    UInt64(u64),
    /// u128
    UInt128(u128),
    /// u256
    UInt256([u128; 2]),
    /// i8
    Int8(i8),
    /// i16
    Int16(i16),
    /// i32
    Int32(i32),
    /// i64
    Int64(i64),
    /// i128
    Int128(i128),
    /// i256
    Int256([i128; 2]),
    /// f32
    Float32(f32),
    /// f64
    Float64(f64),
    // > decimals
    // Decimal(p,s)
    // Decimal32(i32),
    // Decimal64(i64),
    // Decimal128(i128),
    /// bool
    Bool(bool),
    /// string
    String(String),
    /// UUID
    UUID([u8; 16]),
    /// Number of days since 01-01-1970
    Date(u16),
    /// Number of days since 01-01-1970 (signed int)
    Date32(i32),
    /// Number of seconds since 01-01-1970
    DateTime(u32),
    /// Number of nanosecs since 01-01-1970
    DateTime64(i64),
    /// Enum8
    Enum8(i8),
    /// Enum16
    Enum16(i16),
    /// Array
    Array(Vec<Value>),
    /// Tuple
    Tuple(Vec<Value>),
    /// Map
    Map(HashMap<String, Value>),
    /// Nested
    Nested(HashMap<String, Value>),
    /// Nullable u8
    NullableUInt8(Option<u8>),
    /// Nullable u16
    NullableUInt16(Option<u16>),
    /// Nullable u32
    NullableUInt32(Option<u32>),
    /// Nullable u64
    NullableUInt64(Option<u64>),
    /// Nullable u128
    NullableUInt128(Option<u128>),
    /// Nullable u256
    NullableUInt256(Option<[u128; 2]>),
    /// Nullable i8
    NullableInt8(Option<i8>),
    /// Nullable i16
    NullableInt16(Option<i16>),
    /// Nullable i32
    NullableInt32(Option<i32>),
    /// Nullable i64
    NullableInt64(Option<i64>),
    /// Nullable i128
    NullableInt128(Option<i128>),
    /// Nullable i258
    NullableInt256(Option<[i128; 2]>),
    /// Nullable f32
    NullableFloat32(Option<f32>),
    /// Nullable f64
    NullableFloat64(Option<f64>),
    // > decimal
    // NullableDecimal32(Option<i32>),
    // NullableDecimal64(Option<i64>),
    // NullableDecimal128(Option<i128>),
    /// Nullable bool
    NullableBool(Option<bool>),
    /// Nullable string
    NullableString(Option<String>),
    /// Nullable UUID
    NullableUUID(Option<[u8; 16]>),
    /// Nullable date
    NullableDate(Option<u16>),
    /// Nullable date32
    NullableDate32(Option<i32>),
    /// Nullable datetime
    NullableDateTime(Option<u32>),
    /// Nullable datetime64
    NullableDateTime64(Option<i64>),
    /// Nullable Enum8
    NullableEnum8(Option<i8>),
    /// Nullable Enum16
    NullableEnum16(Option<i16>),
}

impl Value {
    /// Tries to convert a [Value] into a specific type T
    pub fn try_into<T>(self) -> Result<T, Error>
    where
        T: ChValue,
    {
        T::from_ch_value(self)
    }

    /// Checks if a [Value] corresponds to a type
    pub fn is_same_type_as(&self, ty: &Type) -> bool {
        match self {
            Value::UInt8(_) => matches!(ty, Type::UInt8),
            Value::UInt16(_) => matches!(ty, Type::UInt16),
            Value::UInt32(_) => matches!(ty, Type::UInt32),
            Value::UInt64(_) => matches!(ty, Type::UInt64),
            Value::UInt128(_) => matches!(ty, Type::UInt128),
            Value::UInt256(_) => matches!(ty, Type::UInt256),
            Value::Int8(_) => matches!(ty, Type::Int8),
            Value::Int16(_) => matches!(ty, Type::Int16),
            Value::Int32(_) => matches!(ty, Type::Int32),
            Value::Int64(_) => matches!(ty, Type::Int64),
            Value::Int128(_) => matches!(ty, Type::Int128),
            Value::Int256(_) => matches!(ty, Type::Int256),
            Value::Float32(_) => matches!(ty, Type::Float32),
            Value::Float64(_) => matches!(ty, Type::Float64),
            Value::Bool(_) => matches!(ty, Type::Bool),
            Value::String(_) => matches!(ty, Type::String),
            Value::UUID(_) => matches!(ty, Type::UUID),
            Value::Date(_) => matches!(ty, Type::Date),
            Value::Date32(_) => matches!(ty, Type::Date32),
            Value::DateTime(_) => matches!(ty, Type::DateTime),
            Value::DateTime64(_) => matches!(ty, Type::DateTime64(_)),
            Value::Enum8(i) => match ty {
                Type::Enum8(variants) => variants.values().any(|v| v == i),
                _ => false,
            },
            Value::Enum16(i) => match ty {
                Type::Enum16(variants) => variants.values().any(|v| v == i),
                _ => false,
            },
            Value::Array(values) => match ty {
                Type::Array(arr_ty) => values.iter().all(|v| v.is_same_type_as(arr_ty.as_ref())),
                _ => false,
            },
            Value::Tuple(values) => match ty {
                Type::Tuple(types) => {
                    if values.len() != types.len() {
                        return false;
                    }
                    values.iter().zip(types).all(|(n, t)| n.is_same_type_as(t))
                }
                _ => false,
            },
            Value::Map(map) => match ty {
                Type::Map(_key_ty, val_ty) => {
                    map.values().all(|v| v.is_same_type_as(val_ty.as_ref()))
                }
                _ => false,
            },
            Value::Nested(values) => match ty {
                Type::Nested(fields) => {
                    let keys = fields.iter().map(|(k, _v)| k.as_str()).collect::<Vec<_>>();
                    values.keys().all(|k| keys.contains(&k.as_str()))
                        && values
                            .iter()
                            .zip(fields)
                            .all(|((_, v), (_, t))| v.is_same_type_as(t))
                }
                _ => false,
            },
            Value::NullableUInt8(_) => matches!(ty, Type::NullableUInt8),
            Value::NullableUInt16(_) => matches!(ty, Type::NullableUInt16),
            Value::NullableUInt32(_) => matches!(ty, Type::NullableUInt32),
            Value::NullableUInt64(_) => matches!(ty, Type::NullableUInt64),
            Value::NullableUInt128(_) => matches!(ty, Type::NullableUInt128),
            Value::NullableUInt256(_) => matches!(ty, Type::NullableUInt256),
            Value::NullableInt8(_) => matches!(ty, Type::NullableInt8),
            Value::NullableInt16(_) => matches!(ty, Type::NullableInt16),
            Value::NullableInt32(_) => matches!(ty, Type::NullableInt32),
            Value::NullableInt64(_) => matches!(ty, Type::NullableInt64),
            Value::NullableInt128(_) => matches!(ty, Type::NullableInt128),
            Value::NullableInt256(_) => matches!(ty, Type::NullableInt256),
            Value::NullableFloat32(_) => matches!(ty, Type::NullableFloat32),
            Value::NullableFloat64(_) => matches!(ty, Type::NullableFloat64),
            Value::NullableBool(_) => matches!(ty, Type::NullableBool),
            Value::NullableString(_) => matches!(ty, Type::NullableString),
            Value::NullableUUID(_) => matches!(ty, Type::NullableUUID),
            Value::NullableDate(_) => matches!(ty, Type::NullableDate),
            Value::NullableDate32(_) => matches!(ty, Type::NullableDate32),
            Value::NullableDateTime(_) => matches!(ty, Type::NullableDateTime),
            Value::NullableDateTime64(_) => matches!(ty, Type::NullableDateTime64(_)),
            Value::NullableEnum8(_) => matches!(ty, Type::NullableEnum8(_)),
            Value::NullableEnum16(_) => matches!(ty, Type::NullableEnum16(_)),
        }
    }

    /// Returns the nullable variant of a value
    pub(crate) fn into_nullable(self) -> Option<Value> {
        match self {
            Value::UInt8(v) => Some(Value::NullableUInt8(Some(v))),
            Value::UInt16(v) => Some(Value::NullableUInt16(Some(v))),
            Value::UInt32(v) => Some(Value::NullableUInt32(Some(v))),
            Value::UInt64(v) => Some(Value::NullableUInt64(Some(v))),
            Value::UInt128(v) => Some(Value::NullableUInt128(Some(v))),
            Value::UInt256(v) => Some(Value::NullableUInt256(Some(v))),
            Value::Int8(v) => Some(Value::NullableInt8(Some(v))),
            Value::Int16(v) => Some(Value::NullableInt16(Some(v))),
            Value::Int32(v) => Some(Value::NullableInt32(Some(v))),
            Value::Int64(v) => Some(Value::NullableInt64(Some(v))),
            Value::Int128(v) => Some(Value::NullableInt128(Some(v))),
            Value::Int256(v) => Some(Value::NullableInt256(Some(v))),
            Value::Float32(v) => Some(Value::NullableFloat32(Some(v))),
            Value::Float64(v) => Some(Value::NullableFloat64(Some(v))),
            Value::Bool(v) => Some(Value::NullableBool(Some(v))),
            Value::String(v) => Some(Value::NullableString(Some(v))),
            Value::UUID(v) => Some(Value::NullableUUID(Some(v))),
            Value::Date(v) => Some(Value::NullableDate(Some(v))),
            Value::Date32(v) => Some(Value::NullableDate32(Some(v))),
            Value::DateTime(v) => Some(Value::NullableDateTime(Some(v))),
            Value::DateTime64(v) => Some(Value::NullableDateTime64(Some(v))),
            Value::Enum8(v) => Some(Value::NullableEnum8(Some(v))),
            Value::Enum16(v) => Some(Value::NullableEnum16(Some(v))),
            Value::Array(_) => None,
            Value::Tuple(_) => None,
            Value::Map(_) => None,
            Value::Nested(_) => None,
            // > nullable
            _ => Some(self),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        /// Implements the nullable variant for formatting
        macro_rules! impl_nullable {
            ($VAL:expr, $VAR:ident) => {
                match $VAL {
                    Some(v) => Value::$VAR(v).to_string(),
                    None => "NULL".to_string(),
                }
            };
        }

        let s = match self {
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
            Value::Bool(v) => v.to_string(),
            Value::String(v) => v.to_string(),
            Value::UUID(_) => {
                let uuid = self.clone().try_into::<Uuid>().unwrap();
                uuid.to_string()
            }
            Value::Date(_) | Value::Date32(_) => {
                let date = self.clone().try_into::<Date>().unwrap();
                date.format_yyyy_mm_dd()
            }
            Value::DateTime(_) => {
                let dt = self.clone().try_into::<OffsetDateTime>().unwrap();
                dt.format_yyyy_mm_dd_hh_mm_ss()
            }
            Value::DateTime64(_) => {
                let dt = self.clone().try_into::<OffsetDateTime>().unwrap();
                dt.format_yyyy_mm_dd_hh_mm_ss_ns()
            }
            Value::Enum8(v) => v.to_string(),
            Value::Enum16(v) => v.to_string(),
            Value::Array(v) => {
                format!(
                    "[{}]",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Tuple(v) => {
                format!(
                    "({})",
                    v.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Map(v) => {
                format!(
                    "{{{}}}",
                    v.iter()
                        .map(|(k, v)| format!("{}: {}", k, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Value::Nested(v) => {
                format!(
                    "{{{}}}",
                    v.iter()
                        .map(|(k, v)| format!("{}: {}", k, v))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
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
        };

        write!(f, "{s}")
    }
}

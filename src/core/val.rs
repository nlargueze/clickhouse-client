//! Value

use std::collections::HashMap;

/// A database value
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum Value {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    UInt256([u8; 32]),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Int256([u8; 32]),
    Float32(f32),
    Float64(f64),
    Decimal32(i32),
    Decimal64(i64),
    Decimal128(i128),
    Bool(bool),
    String(String),
    FixedString(String),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    UUID([u8; 16]),
    Enum8(i8),
    Enum16(i16),
    Array(Vec<Box<Value>>),
    Map(HashMap<String, Box<Value>>),
    Nested(HashMap<String, Box<Value>>),
    NullableUInt8(Option<u8>),
    NullableUInt16(Option<u16>),
    NullableUInt32(Option<u32>),
    NullableUInt64(Option<u64>),
    NullableUInt128(Option<u128>),
    NullableUInt256(Option<[u8; 32]>),
    NullableInt8(Option<i8>),
    NullableInt16(Option<i16>),
    NullableInt32(Option<i32>),
    NullableInt64(Option<i64>),
    NullableInt128(Option<i128>),
    NullableInt256(Option<[u8; 32]>),
    NullableFloat32(Option<f32>),
    NullableFloat64(Option<f64>),
    NullableDecimal32(Option<i32>),
    NullableDecimal64(Option<i64>),
    NullableDecimal128(Option<i128>),
    NullableBool(Option<bool>),
    NullableString(Option<String>),
    NullableFixedString(Option<String>),
    NullableDate(Option<u16>),
    NullableDate32(Option<i32>),
    NullableDateTime(Option<u32>),
    NullableDateTime64(Option<i64>),
    NullableUUID(Option<[u8; 16]>),
}

/// Implements the conversion
macro_rules! impl_conv {
    ($VAR:tt, $TY:ty) => {
        impl From<$TY> for Value {
            fn from(value: $TY) -> Self {
                Value::$VAR(value)
            }
        }
    };
}

impl_conv!(UInt8, u8);
impl_conv!(UInt16, u16);
impl_conv!(UInt32, u32);
impl_conv!(UInt64, u64);
impl_conv!(UInt128, u128);
impl_conv!(Int8, i8);
impl_conv!(Int16, i16);
impl_conv!(Int32, i32);
impl_conv!(Int64, i64);
impl_conv!(Int128, i128);
impl_conv!(Float32, f32);
impl_conv!(Float64, f64);
impl_conv!(Bool, bool);
impl_conv!(String, String);
impl_conv!(NullableUInt8, Option<u8>);
impl_conv!(NullableUInt16, Option<u16>);
impl_conv!(NullableUInt32, Option<u32>);
impl_conv!(NullableUInt64, Option<u64>);
impl_conv!(NullableUInt128, Option<u128>);
impl_conv!(NullableInt8, Option<i8>);
impl_conv!(NullableInt16, Option<i16>);
impl_conv!(NullableInt32, Option<i32>);
impl_conv!(NullableInt64, Option<i64>);
impl_conv!(NullableInt128, Option<i128>);
impl_conv!(NullableFloat32, Option<f32>);
impl_conv!(NullableFloat64, Option<f64>);
impl_conv!(NullableBool, Option<bool>);
impl_conv!(NullableString, Option<String>);

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl Value {
    /// Returns the nullable variant of the same value
    ///
    /// # Returns
    ///
    /// Returns `None` if there is no nullable variant or if it is already nullable
    pub fn as_nullable(&self) -> Option<Self> {
        match self {
            Value::UInt8(x) => Some(Value::NullableUInt8(Some(*x))),
            Value::UInt16(x) => Some(Value::NullableUInt16(Some(*x))),
            Value::UInt32(x) => Some(Value::NullableUInt32(Some(*x))),
            Value::UInt64(x) => Some(Value::NullableUInt64(Some(*x))),
            Value::UInt128(x) => Some(Value::NullableUInt128(Some(*x))),
            Value::UInt256(x) => Some(Value::NullableUInt256(Some(*x))),
            Value::Int8(x) => Some(Value::NullableInt8(Some(*x))),
            Value::Int16(x) => Some(Value::NullableInt16(Some(*x))),
            Value::Int32(x) => Some(Value::NullableInt32(Some(*x))),
            Value::Int64(x) => Some(Value::NullableInt64(Some(*x))),
            Value::Int128(x) => Some(Value::NullableInt128(Some(*x))),
            Value::Int256(x) => Some(Value::NullableInt256(Some(*x))),
            Value::Float32(x) => Some(Value::NullableFloat32(Some(*x))),
            Value::Float64(x) => Some(Value::NullableFloat64(Some(*x))),
            Value::Decimal32(x) => Some(Value::NullableDecimal32(Some(*x))),
            Value::Decimal64(x) => Some(Value::NullableDecimal64(Some(*x))),
            Value::Decimal128(x) => Some(Value::NullableDecimal128(Some(*x))),
            Value::Bool(x) => Some(Value::NullableBool(Some(*x))),
            Value::String(x) => Some(Value::NullableString(Some(x.clone()))),
            Value::FixedString(x) => Some(Value::NullableFixedString(Some(x.clone()))),
            Value::Date(x) => Some(Value::NullableDate(Some(*x))),
            Value::Date32(x) => Some(Value::NullableDate32(Some(*x))),
            Value::DateTime(x) => Some(Value::NullableDateTime(Some(*x))),
            Value::DateTime64(x) => Some(Value::NullableDateTime64(Some(*x))),
            Value::UUID(x) => Some(Value::NullableUUID(Some(*x))),
            Value::Enum8(_) => None,
            Value::Enum16(_) => None,
            Value::Array(_) => None,
            Value::Map(_) => None,
            Value::Nested(_) => None,
            Value::NullableUInt8(_) => None,
            Value::NullableUInt16(_) => None,
            Value::NullableUInt32(_) => None,
            Value::NullableUInt64(_) => None,
            Value::NullableUInt128(_) => None,
            Value::NullableUInt256(_) => None,
            Value::NullableInt8(_) => None,
            Value::NullableInt16(_) => None,
            Value::NullableInt32(_) => None,
            Value::NullableInt64(_) => None,
            Value::NullableInt128(_) => None,
            Value::NullableInt256(_) => None,
            Value::NullableFloat32(_) => None,
            Value::NullableFloat64(_) => None,
            Value::NullableDecimal32(_) => None,
            Value::NullableDecimal64(_) => None,
            Value::NullableDecimal128(_) => None,
            Value::NullableBool(_) => None,
            Value::NullableString(_) => None,
            Value::NullableFixedString(_) => None,
            Value::NullableDate(_) => None,
            Value::NullableDate32(_) => None,
            Value::NullableDateTime(_) => None,
            Value::NullableDateTime64(_) => None,
            Value::NullableUUID(_) => None,
        }
    }
}

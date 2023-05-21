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
    Bool(bool),
    String(String),
    FixedString(String),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    UUID([u8; 16]),
    Array(Vec<Box<Value>>),
    Map(HashMap<String, Box<Value>>),
    Nested(HashMap<String, Box<Value>>),
    NullableUInt8(Option<u8>),
    NullableUInt16(Option<u16>),
    NullableUInt32(Option<u32>),
    NullableUInt64(Option<u64>),
    NullableUInt128(Option<u128>),
    NullableInt8(Option<i8>),
    NullableInt16(Option<i16>),
    NullableInt32(Option<i32>),
    NullableInt64(Option<i64>),
    NullableInt128(Option<i128>),
    NullableFloat32(Option<f32>),
    NullableFloat64(Option<f64>),
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

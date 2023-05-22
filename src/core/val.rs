//! Value

use std::collections::HashMap;

use super::Type;

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
    // NB: Decimal(p,s) is not implemented
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
    NullableEnum8(Option<i8>),
    NullableEnum16(Option<i16>),
}

/// Implements the conversion from base type
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
    /// Returns `None` if there is no nullable variant or itself if it is already nullable
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
            | Value::NullableEnum16(_) => Some(self.clone()),
        }
    }

    /// Returns the non nullable variant of the same value
    ///
    /// # Result
    ///
    /// Returns `None` if the value is nullable and NULL
    pub fn as_non_nullable(&self) -> Option<Self> {
        match self {
            Value::UInt8(_)
            | Value::UInt16(_)
            | Value::UInt32(_)
            | Value::UInt64(_)
            | Value::UInt128(_)
            | Value::UInt256(_)
            | Value::Int8(_)
            | Value::Int16(_)
            | Value::Int32(_)
            | Value::Int64(_)
            | Value::Int128(_)
            | Value::Int256(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Decimal32(_)
            | Value::Decimal64(_)
            | Value::Decimal128(_)
            | Value::Bool(_)
            | Value::String(_)
            | Value::FixedString(_)
            | Value::Date(_)
            | Value::Date32(_)
            | Value::DateTime(_)
            | Value::DateTime64(_)
            | Value::UUID(_)
            | Value::Enum8(_)
            | Value::Enum16(_)
            | Value::Array(_)
            | Value::Map(_)
            | Value::Nested(_) => Some(self.clone()),
            Value::NullableUInt8(x) => x.as_ref().map(|x| Value::UInt8(*x)),
            Value::NullableUInt16(x) => x.as_ref().map(|x| Value::UInt16(*x)),
            Value::NullableUInt32(x) => x.as_ref().map(|x| Value::UInt32(*x)),
            Value::NullableUInt64(x) => x.as_ref().map(|x| Value::UInt64(*x)),
            Value::NullableUInt128(x) => x.as_ref().map(|x| Value::UInt128(*x)),
            Value::NullableUInt256(x) => x.as_ref().map(|x| Value::UInt256(*x)),
            Value::NullableInt8(x) => x.as_ref().map(|x| Value::Int8(*x)),
            Value::NullableInt16(x) => x.as_ref().map(|x| Value::Int16(*x)),
            Value::NullableInt32(x) => x.as_ref().map(|x| Value::Int32(*x)),
            Value::NullableInt64(x) => x.as_ref().map(|x| Value::Int64(*x)),
            Value::NullableInt128(x) => x.as_ref().map(|x| Value::Int128(*x)),
            Value::NullableInt256(x) => x.as_ref().map(|x| Value::Int256(*x)),
            Value::NullableFloat32(x) => x.as_ref().map(|x| Value::Float32(*x)),
            Value::NullableFloat64(x) => x.as_ref().map(|x| Value::Float64(*x)),
            Value::NullableDecimal32(x) => x.as_ref().map(|x| Value::Decimal32(*x)),
            Value::NullableDecimal64(x) => x.as_ref().map(|x| Value::Decimal64(*x)),
            Value::NullableDecimal128(x) => x.as_ref().map(|x| Value::Decimal128(*x)),
            Value::NullableBool(x) => x.as_ref().map(|x| Value::Bool(*x)),
            Value::NullableString(x) => x.as_ref().map(|x| Value::String(x.clone())),
            Value::NullableFixedString(x) => x.as_ref().map(|x| Value::FixedString(x.clone())),
            Value::NullableDate(x) => x.as_ref().map(|x| Value::Date(*x)),
            Value::NullableDate32(x) => x.as_ref().map(|x| Value::Date32(*x)),
            Value::NullableDateTime(x) => x.as_ref().map(|x| Value::DateTime(*x)),
            Value::NullableDateTime64(x) => x.as_ref().map(|x| Value::DateTime64(*x)),
            Value::NullableUUID(x) => x.as_ref().map(|x| Value::UUID(*x)),
            Value::NullableEnum8(x) => x.as_ref().map(|x| Value::Enum8(*x)),
            Value::NullableEnum16(x) => x.as_ref().map(|x| Value::Enum16(*x)),
        }
    }

    /// Returns the NUlL value for a Nullable type
    ///
    /// # Result
    ///
    /// Returns `None` is the type is not nullable
    pub fn null(ty: Type) -> Option<Value> {
        match ty {
            Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::Float32
            | Type::Float64
            | Type::Decimal(_, _)
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Bool
            | Type::String
            | Type::FixedString(_)
            | Type::Date
            | Type::Date32
            | Type::DateTime
            | Type::DateTime64(_)
            | Type::UUID
            | Type::Enum(_)
            | Type::Enum8(_)
            | Type::Enum16(_)
            | Type::Array(_)
            | Type::Map(_, _)
            | Type::Nested(_)
            | Type::Tuple(_) => None,
            Type::NullableUInt8 => Some(Value::NullableUInt8(None)),
            Type::NullableUInt16 => Some(Value::NullableUInt16(None)),
            Type::NullableUInt32 => Some(Value::NullableUInt32(None)),
            Type::NullableUInt64 => Some(Value::NullableUInt64(None)),
            Type::NullableUInt128 => Some(Value::NullableUInt128(None)),
            Type::NullableUInt256 => Some(Value::NullableInt8(None)),
            Type::NullableInt8 => Some(Value::NullableInt8(None)),
            Type::NullableInt16 => Some(Value::NullableInt16(None)),
            Type::NullableInt32 => Some(Value::NullableInt32(None)),
            Type::NullableInt64 => Some(Value::NullableInt64(None)),
            Type::NullableInt128 => Some(Value::NullableInt128(None)),
            Type::NullableInt256 => Some(Value::NullableInt256(None)),
            Type::NullableFloat32 => Some(Value::NullableFloat32(None)),
            Type::NullableFloat64 => Some(Value::NullableFloat64(None)),
            Type::NullableDecimal(_, _) => unimplemented!("Decimal value"),
            Type::NullableDecimal32(_) => Some(Value::NullableDecimal32(None)),
            Type::NullableDecimal64(_) => Some(Value::NullableDecimal64(None)),
            Type::NullableDecimal128(_) => Some(Value::NullableDecimal128(None)),
            Type::NullableDecimal256(_) => unimplemented!("Decimal256 value"),
            Type::NullableBool => Some(Value::NullableBool(None)),
            Type::NullableString => Some(Value::NullableString(None)),
            Type::NullableFixedString(_) => Some(Value::NullableFixedString(None)),
            Type::NullableDate => Some(Value::NullableDate(None)),
            Type::NullableDate32 => Some(Value::NullableDate32(None)),
            Type::NullableDateTime => Some(Value::NullableDateTime(None)),
            Type::NullableDateTime64(_) => Some(Value::NullableDateTime64(None)),
            Type::NullableUUID => Some(Value::NullableUUID(None)),
            Type::NullableEnum(_) => unimplemented!("NullableEnum value"),
            Type::NullableEnum8(_) => Some(Value::NullableEnum8(None)),
            Type::NullableEnum16(_) => Some(Value::NullableEnum16(None)),
        }
    }
}

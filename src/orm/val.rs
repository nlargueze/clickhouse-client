//! DB value

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::Type;

/// A database value
#[derive(Debug, Clone, PartialEq, Deserialize)]
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
macro_rules! impl_conversion {
    ($VAR:tt, $TY:ty) => {
        impl From<$TY> for Value {
            fn from(value: $TY) -> Self {
                Value::$VAR(value)
            }
        }
    };
}

impl_conversion!(UInt8, u8);
impl_conversion!(UInt16, u16);
impl_conversion!(UInt32, u32);
impl_conversion!(UInt64, u64);
impl_conversion!(UInt128, u128);
impl_conversion!(Int8, i8);
impl_conversion!(Int16, i16);
impl_conversion!(Int32, i32);
impl_conversion!(Int64, i64);
impl_conversion!(Int128, i128);
impl_conversion!(Float32, f32);
impl_conversion!(Float64, f64);
impl_conversion!(Bool, bool);
impl_conversion!(String, String);
impl_conversion!(NullableUInt8, Option<u8>);
impl_conversion!(NullableUInt16, Option<u16>);
impl_conversion!(NullableUInt32, Option<u32>);
impl_conversion!(NullableUInt64, Option<u64>);
impl_conversion!(NullableUInt128, Option<u128>);
impl_conversion!(NullableInt8, Option<i8>);
impl_conversion!(NullableInt16, Option<i16>);
impl_conversion!(NullableInt32, Option<i32>);
impl_conversion!(NullableInt64, Option<i64>);
impl_conversion!(NullableInt128, Option<i128>);
impl_conversion!(NullableFloat32, Option<f32>);
impl_conversion!(NullableFloat64, Option<f64>);
impl_conversion!(NullableBool, Option<bool>);
impl_conversion!(NullableString, Option<String>);

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::UInt8(u) => u.serialize(serializer),
            Value::UInt16(u) => u.serialize(serializer),
            Value::UInt32(u) => u.serialize(serializer),
            Value::UInt64(u) => u.serialize(serializer),
            Value::UInt128(u) => u.serialize(serializer),
            Value::UInt256(u) => u.serialize(serializer),
            Value::Int8(i) => i.serialize(serializer),
            Value::Int16(i) => i.serialize(serializer),
            Value::Int32(i) => i.serialize(serializer),
            Value::Int64(i) => i.serialize(serializer),
            Value::Int128(i) => i.serialize(serializer),
            Value::Int256(u) => u.serialize(serializer),
            Value::Float32(f) => f.serialize(serializer),
            Value::Float64(f) => f.serialize(serializer),
            Value::Bool(b) => b.serialize(serializer),
            Value::String(s) => s.serialize(serializer),
            Value::FixedString(fs) => fs.serialize(serializer),
            Value::Date(d) => {
                #[cfg(feature = "time")]
                {
                    use super::time::FORMAT_DATE;

                    let date = time::Date::from_calendar_date(1970, time::Month::January, 1)
                        .unwrap()
                        + time::Duration::days((*d).into());
                    if serializer.is_human_readable() {
                        let date_str = date.format(FORMAT_DATE).expect("invalid date");
                        serializer.serialize_str(&date_str)
                    } else {
                        serializer.serialize_u16(*d)
                    }
                }
                #[cfg(not(feature = "time"))]
                {
                    serializer.serialize_u16(*d)
                }
            }
            Value::Date32(d) => {
                #[cfg(feature = "time")]
                {
                    use super::time::FORMAT_DATE;

                    let date = time::Date::from_calendar_date(1970, time::Month::January, 1)
                        .unwrap()
                        + time::Duration::days((*d).into());
                    if serializer.is_human_readable() {
                        let date_str = date.format(FORMAT_DATE).expect("invalid date");
                        serializer.serialize_str(&date_str)
                    } else {
                        serializer.serialize_i32(*d)
                    }
                }
                #[cfg(not(feature = "time"))]
                {
                    serializer.serialize_i32(*d)
                }
            }
            Value::DateTime(dt) => {
                #[cfg(feature = "time")]
                {
                    use super::time::FORMAT_DATETIME;

                    let date = time::OffsetDateTime::from_unix_timestamp((*dt).into()).unwrap();
                    if serializer.is_human_readable() {
                        let date_str = date.format(FORMAT_DATETIME).expect("invalid date");
                        serializer.serialize_str(&date_str)
                    } else {
                        serializer.serialize_u32(*dt)
                    }
                }
                #[cfg(not(feature = "time"))]
                {
                    serializer.serialize_u32(*dt)
                }
            }
            Value::DateTime64(dt) => {
                #[cfg(feature = "time")]
                {
                    use super::time::FORMAT_DATETIME64;

                    let date =
                        time::OffsetDateTime::from_unix_timestamp_nanos((*dt).into()).unwrap();
                    if serializer.is_human_readable() {
                        let date_str = date.format(FORMAT_DATETIME64).expect("invalid date");
                        serializer.serialize_str(&date_str)
                    } else {
                        serializer.serialize_i64(*dt)
                    }
                }
                #[cfg(not(feature = "time"))]
                {
                    serializer.serialize_i64(*dt)
                }
            }
            Value::UUID(id) => {
                #[cfg(feature = "uuid")]
                {
                    let uuid = ::uuid::Uuid::from_bytes(*id);
                    if serializer.is_human_readable() {
                        uuid.serialize(serializer)
                    } else {
                        serializer.serialize_bytes(id)
                    }
                }
                #[cfg(not(feature = "uuid"))]
                {
                    serializer.serialize_bytes(id)
                }
            }
            Value::Array(arr) => arr.serialize(serializer),
            Value::Map(map) => map.serialize(serializer),
            Value::Nested(nested) => nested.serialize(serializer),
            Value::NullableUInt8(u) => match u {
                Some(u) => serializer.serialize_some(&Value::UInt8(*u)),
                None => serializer.serialize_none(),
            },
            Value::NullableUInt16(u) => match u {
                Some(u) => serializer.serialize_some(&Value::UInt16(*u)),
                None => serializer.serialize_none(),
            },
            Value::NullableUInt32(u) => match u {
                Some(u) => serializer.serialize_some(&Value::UInt32(*u)),
                None => serializer.serialize_none(),
            },
            Value::NullableUInt64(u) => match u {
                Some(u) => serializer.serialize_some(&Value::UInt64(*u)),
                None => serializer.serialize_none(),
            },
            Value::NullableUInt128(u) => match u {
                Some(u) => serializer.serialize_some(&Value::UInt128(*u)),
                None => serializer.serialize_none(),
            },
            Value::NullableInt8(i) => match i {
                Some(i) => serializer.serialize_some(&Value::Int8(*i)),
                None => serializer.serialize_none(),
            },
            Value::NullableInt16(i) => match i {
                Some(i) => serializer.serialize_some(&Value::Int16(*i)),
                None => serializer.serialize_none(),
            },
            Value::NullableInt32(i) => match i {
                Some(i) => serializer.serialize_some(&Value::Int32(*i)),
                None => serializer.serialize_none(),
            },
            Value::NullableInt64(i) => match i {
                Some(i) => serializer.serialize_some(&Value::Int64(*i)),
                None => serializer.serialize_none(),
            },
            Value::NullableInt128(i) => match i {
                Some(i) => serializer.serialize_some(&Value::Int128(*i)),
                None => serializer.serialize_none(),
            },
            Value::NullableFloat32(f) => match f {
                Some(f) => serializer.serialize_some(&Value::Float32(*f)),
                None => serializer.serialize_none(),
            },
            Value::NullableFloat64(f) => match f {
                Some(f) => serializer.serialize_some(&Value::Float64(*f)),
                None => serializer.serialize_none(),
            },
            Value::NullableBool(b) => match b {
                Some(b) => serializer.serialize_some(&Value::Bool(*b)),
                None => serializer.serialize_none(),
            },
            Value::NullableString(s) => match s {
                Some(s) => serializer.serialize_some(&Value::String(s.to_string())),
                None => serializer.serialize_none(),
            },
            Value::NullableFixedString(s) => match s {
                Some(s) => serializer.serialize_some(&Value::String(s.to_string())),
                None => serializer.serialize_none(),
            },
            Value::NullableDate(d) => match d {
                Some(d) => serializer.serialize_some(&Value::Date(*d)),
                None => serializer.serialize_none(),
            },
            Value::NullableDate32(d) => match d {
                Some(d) => serializer.serialize_some(&Value::Date32(*d)),
                None => serializer.serialize_none(),
            },
            Value::NullableDateTime(dt) => match dt {
                Some(dt) => serializer.serialize_some(&Value::DateTime(*dt)),
                None => serializer.serialize_none(),
            },
            Value::NullableDateTime64(dt) => match dt {
                Some(dt) => serializer.serialize_some(&Value::DateTime64(*dt)),
                None => serializer.serialize_none(),
            },
            Value::NullableUUID(id) => match id {
                Some(id) => serializer.serialize_some(&Value::UUID(*id)),
                None => serializer.serialize_none(),
            },
        }
    }
}

/// Serde visitor for Uint256 [u8; 256]
#[derive(Debug)]
struct FixedBytesVisitor<const N: usize>;

impl<'de, const N: usize> serde::de::Visitor<'de> for FixedBytesVisitor<N> {
    type Value = [u8; N];

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "N bytes to represent fixed bytes types")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() != N {
            return Err(E::custom("FixedBytesVisitor must have N bytes"));
        }
        let mut arr = [0x00; N];
        (0..N).for_each(|i| {
            arr[i] = *v.get(i).unwrap();
        });
        Ok(arr)
    }
}

impl Value {
    /// Deserialize a specific type
    pub(crate) fn deserialize_type<'de, D>(deserializer: D, r#type: Type) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match r#type {
            Type::UInt8 => {
                let u = u8::deserialize(deserializer)?;
                Ok(Value::UInt8(u))
            }
            Type::UInt16 => {
                let u = u16::deserialize(deserializer)?;
                Ok(Value::UInt16(u))
            }
            Type::UInt32 => {
                let u = u32::deserialize(deserializer)?;
                Ok(Value::UInt32(u))
            }
            Type::UInt64 => {
                let u = u64::deserialize(deserializer)?;
                Ok(Value::UInt64(u))
            }
            Type::UInt128 => {
                let u = u128::deserialize(deserializer)?;
                Ok(Value::UInt128(u))
            }
            Type::UInt256 => {
                let bytes = deserializer.deserialize_bytes(FixedBytesVisitor::<32>)?;
                Ok(Value::UInt256(bytes))
            }
            Type::Int8 => {
                let i = i8::deserialize(deserializer)?;
                Ok(Value::Int8(i))
            }
            Type::Int16 => {
                let i = i16::deserialize(deserializer)?;
                Ok(Value::Int16(i))
            }
            Type::Int32 => {
                let i = i32::deserialize(deserializer)?;
                Ok(Value::Int32(i))
            }
            Type::Int64 => {
                let i = i64::deserialize(deserializer)?;
                Ok(Value::Int64(i))
            }
            Type::Int128 => {
                let i = i128::deserialize(deserializer)?;
                Ok(Value::Int128(i))
            }
            Type::Int256 => {
                let bytes = deserializer.deserialize_bytes(FixedBytesVisitor::<32>)?;
                Ok(Value::Int256(bytes))
            }
            Type::Float32 => {
                let f = f32::deserialize(deserializer)?;
                Ok(Value::Float32(f))
            }
            Type::Float64 => {
                let f = f64::deserialize(deserializer)?;
                Ok(Value::Float64(f))
            }
            Type::Decimal(_, _) => {
                unimplemented!("deserializing to Decimal(P,S)")
            }
            Type::Decimal32(_) => {
                unimplemented!("deserializing to Decimal32")
            }
            Type::Decimal64(_) => {
                unimplemented!("deserializing to Decimal64")
            }
            Type::Decimal128(_) => {
                unimplemented!("deserializing to Decimal128")
            }
            Type::Decimal256(_) => {
                unimplemented!("deserializing to Decimal256")
            }
            Type::Bool => {
                let b = bool::deserialize(deserializer)?;
                Ok(Value::Bool(b))
            }
            Type::String => {
                let s = String::deserialize(deserializer)?;
                Ok(Value::String(s))
            }
            Type::FixedString(_) => {
                let s = String::deserialize(deserializer)?;
                Ok(Value::FixedString(s))
            }
            Type::Date => {
                let d = u16::deserialize(deserializer)?;
                Ok(Value::Date(d))
            }
            Type::Date32 => {
                let d = i32::deserialize(deserializer)?;
                Ok(Value::Date32(d))
            }
            Type::DateTime => {
                let dt = u32::deserialize(deserializer)?;
                Ok(Value::DateTime(dt))
            }
            Type::DateTime64(_) => {
                let dt = i64::deserialize(deserializer)?;
                Ok(Value::DateTime64(dt))
            }
            Type::UUID => {
                let bytes = deserializer.deserialize_bytes(FixedBytesVisitor::<16>)?;
                Ok(Value::UUID(bytes))
            }
            Type::Array(_ty) => {
                todo!()
            }
            Type::Map(_, _) => todo!(),
            Type::Nested(_) => todo!(),
            Type::NullableUInt8 => todo!(),
            Type::NullableUInt16 => todo!(),
            Type::NullableUInt32 => todo!(),
            Type::NullableUInt64 => todo!(),
            Type::NullableUInt128 => todo!(),
            Type::NullableUInt256 => todo!(),
            Type::NullableInt8 => todo!(),
            Type::NullableInt16 => todo!(),
            Type::NullableInt32 => todo!(),
            Type::NullableInt64 => todo!(),
            Type::NullableInt128 => todo!(),
            Type::NullableInt256 => todo!(),
            Type::NullableFloat32 => todo!(),
            Type::NullableFloat64 => todo!(),
            Type::NullableDecimal(_, _) => todo!(),
            Type::NullableDecimal32(_) => todo!(),
            Type::NullableDecimal64(_) => todo!(),
            Type::NullableDecimal128(_) => todo!(),
            Type::NullableDecimal256(_) => todo!(),
            Type::NullableBool => todo!(),
            Type::NullableString => todo!(),
            Type::NullableFixedString(_) => todo!(),
            Type::NullableDate => todo!(),
            Type::NullableDate32 => todo!(),
            Type::NullableDateTime => todo!(),
            Type::NullableDateTime64(_) => todo!(),
            Type::NullableUUID => todo!(),
        }
    }
}

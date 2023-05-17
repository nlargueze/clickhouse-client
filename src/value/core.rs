//! Core implementations

use std::collections::{BTreeMap, HashMap};

use impl_trait_for_tuples::impl_for_tuples;

use super::{ChValue, Type, Value};
use crate::error::Error;

/// Implements the [Value] trait for built-in types
macro_rules! impl_ch_type {
    ($TY:ty, $CH_TY:expr, $VAR:ident) => {
        impl ChValue for $TY {
            fn ch_type() -> Type {
                $CH_TY
            }

            fn into_ch_value(self) -> Value {
                Value::$VAR(self)
            }

            fn from_ch_value(value: Value) -> Result<Self, Error> {
                match value {
                    Value::$VAR(v) => Ok(v),
                    _ => Err(Error::new("Cannot convert Value to base type")),
                }
            }
        }
    };
}

// non-nullable std types
impl_ch_type!(u8, Type::UInt8, UInt8);
impl_ch_type!(u16, Type::UInt16, UInt16);
impl_ch_type!(u32, Type::UInt32, UInt32);
impl_ch_type!(u64, Type::UInt64, UInt64);
impl_ch_type!(u128, Type::UInt128, UInt128);
impl_ch_type!(i8, Type::Int8, Int8);
impl_ch_type!(i16, Type::Int16, Int16);
impl_ch_type!(i32, Type::Int32, Int32);
impl_ch_type!(i64, Type::Int64, Int64);
impl_ch_type!(i128, Type::Int128, Int128);
impl_ch_type!(f32, Type::Float32, Float32);
impl_ch_type!(f64, Type::Float64, Float64);
impl_ch_type!(bool, Type::Bool, Bool);
impl_ch_type!(String, Type::String, String);
// nullable std types
impl_ch_type!(Option<u8>, Type::NullableUInt8, NullableUInt8);
impl_ch_type!(Option<u16>, Type::NullableUInt16, NullableUInt16);
impl_ch_type!(Option<u32>, Type::NullableUInt32, NullableUInt32);
impl_ch_type!(Option<u64>, Type::NullableUInt64, NullableUInt64);
impl_ch_type!(Option<u128>, Type::NullableUInt128, NullableUInt128);
impl_ch_type!(Option<i8>, Type::NullableInt8, NullableInt8);
impl_ch_type!(Option<i16>, Type::NullableInt16, NullableInt16);
impl_ch_type!(Option<i32>, Type::NullableInt32, NullableInt32);
impl_ch_type!(Option<i64>, Type::NullableInt64, NullableInt64);
impl_ch_type!(Option<i128>, Type::NullableInt128, NullableInt128);
impl_ch_type!(Option<f32>, Type::NullableFloat32, NullableFloat32);
impl_ch_type!(Option<f64>, Type::NullableFloat64, NullableFloat64);
impl_ch_type!(Option<bool>, Type::NullableBool, NullableBool);
impl_ch_type!(Option<String>, Type::NullableString, NullableString);

// &str
impl ChValue for &str {
    fn ch_type() -> Type {
        Type::String
    }

    fn into_ch_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn from_ch_value(_value: Value) -> Result<Self, Error> {
        unreachable!("&str cannot be parsed from a value")
    }
}

// Vec<T>
impl<T> ChValue for Vec<T>
where
    T: ChValue,
{
    fn ch_type() -> Type {
        Type::Array(Box::new(T::ch_type()))
    }

    fn into_ch_value(self) -> Value {
        Value::Array(self.into_iter().map(|v| v.into_ch_value()).collect())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Array(values) => {
                let mut ts = vec![];
                for value in values {
                    ts.push(T::from_ch_value(value)?);
                }
                Ok(ts)
            }
            _ => Err(Error::new("Cannot convert Value to array ")),
        }
    }
}

// HashMap<String, T>
impl<T> ChValue for HashMap<String, T>
where
    T: ChValue,
{
    fn ch_type() -> Type {
        Type::Map(Box::new(String::ch_type()), Box::new(T::ch_type()))
    }

    fn into_ch_value(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(k, v)| (k, v.into_ch_value()))
                .collect(),
        )
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Map(values) => {
                let mut map = HashMap::new();
                for (key, value) in values {
                    map.insert(key, T::from_ch_value(value)?);
                }
                Ok(map)
            }
            _ => Err(Error::new("Cannot convert Value to array ")),
        }
    }
}

// BTreeMap<String, T>
impl<T> ChValue for BTreeMap<String, T>
where
    T: ChValue,
{
    fn ch_type() -> Type {
        Type::Map(Box::new(String::ch_type()), Box::new(T::ch_type()))
    }

    fn into_ch_value(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(k, v)| (k, v.into_ch_value()))
                .collect(),
        )
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Map(values) => {
                let mut map = BTreeMap::new();
                for (key, value) in values {
                    map.insert(key, T::from_ch_value(value)?);
                }
                Ok(map)
            }
            _ => Err(Error::new("Cannot convert Value to array ")),
        }
    }
}

// (T1, T2, ...)
#[impl_for_tuples(1, 10)]
impl ChValue for Tuple {
    #[allow(clippy::vec_init_then_push)]
    fn ch_type() -> Type {
        let mut types = vec![];
        for_tuples!( #( types.push(Tuple::ch_type()); )* );
        Type::Tuple(types)
    }

    #[allow(clippy::vec_init_then_push)]
    fn into_ch_value(self) -> Value {
        let mut values = vec![];
        for_tuples!( #( values.push(self.Tuple.into_ch_value()); )* );
        Value::Tuple(values)
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Tuple(values) => {
                let mut iter = values.into_iter();
                Ok((for_tuples!( #( Tuple::from_ch_value(iter.next().unwrap())?),* )))
            }
            _ => Err(Error::new("Cannot convert Value to array ")),
        }
    }
}

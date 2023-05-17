//! U256 and I256 integers

use crate::{
    error::Error,
    value::{ChValue, Type, Value},
};

pub use ethnum::{I256, U256};

// -- U256 --
impl ChValue for U256 {
    fn ch_type() -> Type {
        Type::UInt256
    }

    fn into_ch_value(self) -> Value {
        Value::UInt256(self.into_words().into())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::UInt256(v) => Ok(U256::from_words(v[0], v[1])),
            _ => Err(crate::error::Error::new(
                "Cannot convert Value to base type",
            )),
        }
    }
}

impl ChValue for Option<U256> {
    fn ch_type() -> Type {
        Type::NullableUInt256
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => Value::NullableUInt256(Some(v.into_words().into())),
            None => Value::NullableUInt256(None),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableUInt256(v) => Ok(v.map(|x| U256::from_words(x[0], x[1]))),
            _ => Err(crate::error::Error::new(
                "Cannot convert Value to base type",
            )),
        }
    }
}

// -- I256 --
impl ChValue for I256 {
    fn ch_type() -> Type {
        Type::Int256
    }

    fn into_ch_value(self) -> Value {
        Value::Int256(self.into_words().into())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Int256(v) => Ok(I256::from_words(v[0], v[1])),
            _ => Err(crate::error::Error::new(
                "Cannot convert Value to base type",
            )),
        }
    }
}

impl ChValue for Option<I256> {
    fn ch_type() -> Type {
        Type::NullableInt256
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => Value::NullableInt256(Some(v.into_words().into())),
            None => Value::NullableInt256(None),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableInt256(v) => Ok(v.map(|x| I256::from_words(x[0], x[1]))),
            _ => Err(crate::error::Error::new(
                "Cannot convert Value to base type",
            )),
        }
    }
}

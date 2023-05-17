//! Extension for the `uuid` crate

pub use uuid::Uuid;

use crate::{
    error::Error,
    value::{ChValue, Type, Value},
};

impl ChValue for Uuid {
    fn ch_type() -> Type {
        Type::UUID
    }

    fn into_ch_value(self) -> Value {
        Value::UUID(self.into_bytes())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::UUID(v) => Ok(Uuid::from_bytes(v)),
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

impl ChValue for Option<Uuid> {
    fn ch_type() -> Type {
        Type::NullableUUID
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => Value::UUID(v.into_bytes()),
            None => Value::UUID([0; 16]),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableUUID(v) => Ok(v.map(Uuid::from_bytes)),
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

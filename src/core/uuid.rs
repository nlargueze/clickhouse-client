//! Extension for the `uuid` crate

use uuid::Uuid;

use super::{Type, TypeOrm, Value};
use crate::error::Error;

impl TypeOrm for Uuid {
    fn db_type() -> super::Type {
        Type::UUID
    }

    fn db_value(&self) -> Value {
        Value::UUID(self.into_bytes())
    }

    fn from_db_value(value: &Value) -> Result<Self, Error>
    where
        Self: Sized,
    {
        match value {
            Value::UUID(id) => Ok(Uuid::from_bytes(*id)),
            _ => Err(Error("Value is not an UUID".to_string())),
        }
    }
}

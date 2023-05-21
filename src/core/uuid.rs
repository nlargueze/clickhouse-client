//! Extension for the `uuid` crate

use uuid::Uuid;

use super::Value;

impl From<Uuid> for Value {
    fn from(value: Uuid) -> Self {
        Value::UUID(value.into_bytes())
    }
}

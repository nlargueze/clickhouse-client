//! Type ORM

use crate::{
    core::{Type, Value},
    error::Error,
};

/// Extension trait to map a Rust type to a DB type and value
pub trait TypeOrm {
    /// Returns the DB type
    fn db_type() -> Type;

    /// Returns the DB value
    fn db_value(&self) -> Value;

    /// Returns the DB value
    fn from_db_value(value: &Value) -> Result<Self, Error>
    where
        Self: Sized;
}

// implement From trait automatically
impl<T> From<T> for Value
where
    T: TypeOrm,
{
    fn from(value: T) -> Self {
        value.db_value()
    }
}

/// Implement the trait OrmTypeExt for base types
macro_rules! impl_orm_type_ext {
    ($TY:ty, $DB_TY:tt) => {
        impl TypeOrm for $TY {
            fn db_type() -> Type {
                Type::$DB_TY
            }

            fn db_value(&self) -> Value {
                Value::$DB_TY(self.clone())
            }

            fn from_db_value(value: &Value) -> Result<Self, Error>
            where
                Self: Sized,
            {
                match value {
                    Value::$DB_TY(x) => Ok(x.clone()),
                    _ => unreachable!(),
                }
            }
        }
    };
}

impl_orm_type_ext!(u8, UInt8);
impl_orm_type_ext!(u16, UInt16);
impl_orm_type_ext!(u32, UInt32);
impl_orm_type_ext!(u64, UInt64);
impl_orm_type_ext!(u128, UInt128);
impl_orm_type_ext!(i8, Int8);
impl_orm_type_ext!(i16, Int16);
impl_orm_type_ext!(i32, Int32);
impl_orm_type_ext!(i64, Int64);
impl_orm_type_ext!(i128, Int128);
impl_orm_type_ext!(f32, Float32);
impl_orm_type_ext!(f64, Float64);
impl_orm_type_ext!(bool, Bool);
impl_orm_type_ext!(String, String);
impl_orm_type_ext!(Option<u8>, NullableUInt8);
impl_orm_type_ext!(Option<u16>, NullableUInt16);
impl_orm_type_ext!(Option<u32>, NullableUInt32);
impl_orm_type_ext!(Option<u64>, NullableUInt64);
impl_orm_type_ext!(Option<u128>, NullableUInt128);
impl_orm_type_ext!(Option<i8>, NullableInt8);
impl_orm_type_ext!(Option<i16>, NullableInt16);
impl_orm_type_ext!(Option<i32>, NullableInt32);
impl_orm_type_ext!(Option<i64>, NullableInt64);
impl_orm_type_ext!(Option<i128>, NullableInt128);
impl_orm_type_ext!(Option<f32>, NullableFloat32);
impl_orm_type_ext!(Option<f64>, NullableFloat64);
impl_orm_type_ext!(Option<bool>, NullableBool);
impl_orm_type_ext!(Option<String>, NullableString);

impl<'a> TypeOrm for &'a str {
    fn db_type() -> Type {
        Type::String
    }

    fn db_value(&self) -> Value {
        Value::String(self.to_string())
    }

    fn from_db_value(_value: &Value) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Err(Error::new("Value cannot be converted to &str"))
    }
}

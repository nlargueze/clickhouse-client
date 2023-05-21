//! Formats

use std::io::Read;

use super::{Type, Value};

pub mod rowbin;
pub mod sql;

/// Formatter
///
/// A formatter can format a Value into its base representation,
/// and parse the base representation to a Value
pub trait Formatter {
    /// Base type
    type Ok;

    /// Error
    type Err: std::error::Error;

    /// Formats a value
    fn format(&self, value: &Value) -> Self::Ok;

    /// Parses a type value from a buffer
    fn parse(&self, ty: Type, reader: &mut impl Read) -> Result<Value, Self::Err>;
}

impl Value {
    /// Formats the value
    pub fn format<F>(&self, formatter: &F) -> F::Ok
    where
        F: Formatter,
    {
        formatter.format(self)
    }

    /// Parses from a buffer
    pub fn parse<F>(formatter: &F, ty: Type, reader: &mut impl Read) -> Result<Self, F::Err>
    where
        F: Formatter,
    {
        formatter.parse(ty, reader)
    }
}

//! Formats

use std::io::Read;

use crate::query::QueryTable;

use super::{Type, Value};

pub mod rowbin;
pub mod sql;

/// Value formatter
///
/// A formatter can format a Value into its base representation,
/// and parse the base representation to a Value
pub trait Formatter {
    /// Base type
    type Target;

    /// Error
    type Err: std::error::Error;

    /// Formats a value
    fn format(&self, value: &Value) -> Self::Target;

    /// Parses a type value from a buffer
    fn parse(&self, reader: &mut impl Read, ty: Type) -> Result<Value, Self::Err>;
}

impl Value {
    /// Formats the value
    pub fn format<F>(&self, formatter: &F) -> F::Target
    where
        F: Formatter,
    {
        formatter.format(self)
    }

    /// Parses from a buffer
    pub fn parse<F>(formatter: &F, reader: &mut impl Read, ty: Type) -> Result<Self, F::Err>
    where
        F: Formatter,
    {
        formatter.parse(reader, ty)
    }
}

/// Table formatter
///
/// A table formatter formats a QueryTable
pub trait TableFormatter
where
    Self: Formatter,
{
    /// Formats a table
    fn format_table(&self, table: &QueryTable) -> Self::Target;

    /// Parses a type value from a buffer
    ///
    /// # Arguments
    ///
    /// We need to pass the types if the reader does not contain the types
    fn parse_table(
        &self,
        reader: &mut impl Read,
        types: Option<&[&Type]>,
    ) -> Result<QueryTable, Self::Err>;
}

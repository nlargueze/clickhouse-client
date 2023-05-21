//! Clickhouse formats

pub mod rowbin;
pub mod sql;

use crate::orm::{Type, Value};

/// A representation of a data table for formatting
#[derive(Debug, Default)]
pub(crate) struct FmtDataTable {
    /// Names
    pub names: Option<Vec<String>>,
    /// Types
    pub types: Option<Vec<Type>>,
    /// Rows
    ///
    /// The 1st Vec is for rows, the 2nd for each row column
    pub rows: Vec<Vec<Value>>,
}

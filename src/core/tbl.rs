//! Table

use super::{Type, Value};

/// Data table
///
/// A data table represents a view of the data.
///
/// It contains:
/// - an optional header with column names and column types
/// - rows of values
#[derive(Debug, Default)]
pub struct DataTable {
    /// Column names
    pub names: Option<Vec<String>>,
    /// Column types
    pub types: Option<Vec<Type>>,
    /// Rows
    ///
    /// The 1st Vec is for rows, the 2nd for each row column
    pub rows: Vec<Vec<Value>>,
}

impl DataTable {
    /// Returns the number of columns
    pub fn nb_cols(&self) -> Option<usize> {
        self.names.as_ref().map(|names| names.len())
    }
}

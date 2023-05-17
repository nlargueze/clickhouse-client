//! Query result

use crate::{error::Error, value::Type};

use super::{Format, QueryData};

/// Query response
#[derive(Debug, Clone)]
pub struct QueryResponse {
    /// Query format
    pub format: Format,
    /// Raw data
    pub data: Vec<u8>,
}

impl QueryResponse {
    /// Creates a query response
    pub fn new(format: Format, data: Vec<u8>) -> Self {
        Self { format, data }
    }

    /// Converts into a table
    pub fn into_table(self, mapping: Option<&[(&str, Type)]>) -> Result<QueryData, Error> {
        QueryData::from_bytes(&self.data, self.format, mapping)
    }
}

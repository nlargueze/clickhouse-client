//! DDL

mod query;

pub use query::*;

use crate::value::Type;

/// Table schema
#[derive(Debug)]
pub struct TableSchema {
    /// Name
    pub name: String,
    /// Columns
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Creates a new table schema with columns
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: vec![],
        }
    }

    /// Adds a column
    pub fn with_column(mut self, column: ColumnSchema) -> Self {
        self.columns.push(column);
        self
    }

    /// Adds a column with its fields
    pub fn column(mut self, id: &str, ty: Type, primary: bool) -> Self {
        self.columns.push(ColumnSchema::new(id, ty, primary));
        self
    }

    /// Adds a column
    pub fn add_column(&mut self, id: &str, ty: Type, primary: bool) -> &mut Self {
        self.columns.push(ColumnSchema::new(id, ty, primary));
        self
    }

    /// Returns a column by ID
    pub fn get_column_by_id(&self, id: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.id.as_str() == id)
    }
}

/// Column schema
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// ID
    pub id: String,
    /// Type (Clickhouse data type)
    pub ty: Type,
    /// Is a primary key
    pub primary: bool,
}

impl ColumnSchema {
    /// Creates a new column
    pub fn new(id: &str, ty: Type, primary: bool) -> Self {
        Self {
            id: id.to_string(),
            ty,
            primary,
        }
    }
}

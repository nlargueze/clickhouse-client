//! DB schema

mod row;
mod types;

pub use row::*;
pub use types::*;

/// Schema prelude
pub mod prelude {
    pub use super::{ColumnSchema, DbRow, DbRowExt, DbType, Schema, TableSchema};
}

/// DB schema
#[derive(Debug, Default)]
pub struct Schema {
    /// Database name
    pub db_name: String,
    /// Tables
    pub tables: Vec<TableSchema>,
}

impl Schema {
    /// Instantiates a new schema
    pub fn new(db_name: &str) -> Self {
        Self {
            db_name: db_name.to_string(),
            tables: vec![],
        }
    }

    /// Adds a table schema
    pub fn table(mut self, table: TableSchema) -> Self {
        self.tables.push(table);
        self
    }

    /// Returns an immutable reference to a table schema
    pub fn get_table(&self, key: &str) -> Option<&TableSchema> {
        self.tables.iter().find(|t| t.name == key)
    }

    /// Returns a mutable reference to a table schema
    pub fn get_table_mut(&mut self, key: &str) -> Option<&mut TableSchema> {
        self.tables.iter_mut().find(|t| t.name == key)
    }
}

/// Table schema
#[derive(Debug)]
pub struct TableSchema {
    /// Name
    pub name: String,
    /// Columns
    pub cols: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Instantiates
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            cols: vec![],
        }
    }

    /// Adds a column schema
    pub fn column(mut self, col: ColumnSchema) -> Self {
        self.cols.push(col);
        self
    }

    /// Returns an immutable reference to a column schema
    pub fn get_column(&self, key: &str) -> Option<&ColumnSchema> {
        self.cols.iter().find(|c| c.name == key)
    }

    /// Returns a mutable reference to a column schema
    pub fn get_column_mut(&mut self, key: &str) -> Option<&mut ColumnSchema> {
        self.cols.iter_mut().find(|c| c.name == key)
    }
}

/// Column schema
#[derive(Debug)]
pub struct ColumnSchema {
    /// Name
    pub name: String,
    /// Type (Clickhouse data type)
    pub ty: String,
    /// Primary key
    pub is_primary: bool,
}

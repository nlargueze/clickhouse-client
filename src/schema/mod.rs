//! DB schema

use std::collections::HashMap;

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
    pub tables: HashMap<String, TableSchema>,
}

impl Schema {
    /// Instantiates a new schema
    pub fn new(db_name: &str) -> Self {
        Self {
            db_name: db_name.to_string(),
            tables: HashMap::new(),
        }
    }

    /// Adds a table schema
    pub fn table(mut self, table: TableSchema) -> Self {
        self.tables.insert(table.name.clone(), table);
        self
    }

    /// Returns an immutable reference to a table schema
    pub fn get_table(&self, key: &str) -> Option<&TableSchema> {
        self.tables.get(key)
    }

    /// Returns a mutable reference to a table schema
    pub fn get_table_mut(&mut self, key: &str) -> Option<&mut TableSchema> {
        self.tables.get_mut(key)
    }
}

/// Table schema
#[derive(Debug)]
pub struct TableSchema {
    /// Name
    pub name: String,
    /// Columns
    pub cols: HashMap<String, ColumnSchema>,
}

impl TableSchema {
    /// Instantiates
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            cols: HashMap::new(),
        }
    }

    /// Adds a column schema
    pub fn column(mut self, col: ColumnSchema) -> Self {
        self.cols.insert(col.name.clone(), col);
        self
    }

    /// Returns an immutable reference to a column schema
    pub fn get_column(&self, key: &str) -> Option<&ColumnSchema> {
        self.cols.get(key)
    }

    /// Returns a mutable reference to a column schema
    pub fn get_column_mut(&mut self, key: &str) -> Option<&mut ColumnSchema> {
        self.cols.get_mut(key)
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

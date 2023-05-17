//! DB schema

use crate::{error::Error, Client};

/// DB schema
#[derive(Debug, Default)]
pub struct DbSchema {
    /// Tables
    pub tables: Vec<TableSchema>,
}

impl DbSchema {
    /// Instantiates a new schema
    pub fn new() -> Self {
        Self { tables: vec![] }
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

impl Client {
    /// Creates a database
    #[tracing::instrument(skip(self))]
    pub async fn create_db(&self, db: &str) -> Result<(), Error> {
        let query = format!("CREATE DATABASE IF NOT EXISTS {}", db);
        let mut opts = self.send_raw_query_opts();
        opts.db = None;
        let _res_bytes = self.interface.send_raw_query(&query, opts).await?;
        Ok(())
    }

    /// Creates a table
    #[tracing::instrument(skip(self))]
    pub async fn create_table(&self, schema: &TableSchema, engine: &str) -> Result<(), Error> {
        let table = if let Some(db) = &self.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };

        let fields = schema
            .cols
            .iter()
            .map(|col| format!("{} {}", col.name, col.ty))
            .collect::<Vec<_>>()
            .join(", ");

        let keys = schema
            .cols
            .iter()
            .filter_map(|col| {
                if col.is_primary {
                    Some(col.name.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = {} PRIMARY KEY ({})",
            table, fields, engine, keys
        );

        let _res_bytes = self.send_query(query.into()).await?;
        Ok(())
    }
}

//! ORM mapping
//!
//! The module provides mapping between Rust types and Clickhouse types,
//! as well as a mapping between Rust structs and Clickhouse row records.

#[cfg(test)]
mod tests;
mod types;

/// ORM prelude
pub mod prelude {
    pub use super::{DbRecord, DbRecordExt, DbType, DbValue};
    pub use crate::schema::{ColumnSchema, TableSchema};
}

use crate::{error::Error, query::Where, schema::TableSchema, Client};
use std::collections::HashMap;

pub use clickhouse_client_macros::DbRecord;
pub use types::*;

/// Extension trait to represent a Rust struct as a database record
pub trait DbRecordExt {
    /// Returns table schema
    fn db_schema() -> TableSchema;

    /// Returns the DB values
    fn db_values(&self) -> HashMap<&'static str, Box<&'_ dyn DbValue>>;

    /// Parses the row from a map(column, value)
    fn from_db_values(values: HashMap<&str, &str>) -> Result<Self, String>
    where
        Self: Sized + Default;
}

impl Client {
    /// Inserts 1 or several records
    ///
    /// # Arguments
    ///
    /// If no columns are passed, all columns are returned.
    ///
    /// # Returns
    ///
    /// In Clickhouse, there is no RETURNING statement, so nothing is returned.
    #[tracing::instrument(skip_all, fields(records.len = records.len()))]
    pub async fn insert<T>(&self, records: &[T]) -> Result<(), Error>
    where
        T: DbRecordExt,
    {
        let schema: TableSchema = T::db_schema();
        let table = if let Some(db) = &self.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };
        let cols: Vec<_> = schema.cols.iter().map(|col| col.name.as_str()).collect();
        let vals = records
            .iter()
            .map(|record| {
                // iterate over the records
                let values = record.db_values();
                // contains each columns value as string (in the order of columns)
                let mut values_str = vec![];
                for col in cols.iter() {
                    // iterate over the columns
                    values_str.push(values.get(col).unwrap().to_sql_str());
                }
                values_str
            })
            .collect::<Vec<_>>();
        let query = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table,
            cols.join(", "),
            vals.iter()
                .map(|record_vals| { format!("({})", record_vals.join(", ")) })
                .collect::<Vec<String>>()
                .join(", "),
        );

        let _res_bytes = self.send_query(query.into()).await?;
        Ok(())
    }

    /// Selects 1 or several records
    ///
    /// # Arguments
    ///
    /// - is cols is empty, all fields are retrieved
    #[tracing::instrument(skip(self))]
    pub async fn select<T>(&self, cols: &[&str], where_cond: Where) -> Result<Vec<T>, Error>
    where
        T: DbRecordExt + Default,
    {
        let schema = T::db_schema();
        let table = if let Some(db) = &self.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };
        let cols = if cols.is_empty() {
            "*".to_string()
        } else {
            cols.join(", ")
        };
        let query = format!("SELECT {cols} FROM {table}{where_cond} FORMAT TabSeparatedWithNames");

        let res_bytes = self.send_query(query.into()).await?;
        let res_str = String::from_utf8(res_bytes)?;
        // tracing::debug!(query_res = res_str, "returned raw result");

        // parse the DB results
        let mut res_cols = vec![];
        let mut res_values = vec![];
        for (i, line) in res_str.lines().enumerate() {
            if i == 0 {
                res_cols = line.split('\t').collect();
            } else {
                let mut map = HashMap::new();
                for (j, val) in line.split('\t').enumerate() {
                    let col = *res_cols.get(j).expect("shouldn't happen");
                    map.insert(col, val);
                }
                res_values.push(map);
            }
        }
        // tracing::debug!(columns = ?res_cols, values = ?res_values, "results parsed");

        // parse to object T
        let mut records = vec![];
        for map in res_values {
            let record = T::from_db_values(map).map_err(|err| Error::new(err.as_str()))?;
            records.push(record);
        }

        Ok(records)
    }

    /// Updates a record
    ///
    /// # Arguments
    ///
    /// If no columns are provided, all columns are updated
    #[tracing::instrument(skip(self, record))]
    pub async fn update<T>(&self, record: &T, cols: &[&str], where_cond: Where) -> Result<(), Error>
    where
        T: DbRecordExt,
    {
        let schema = T::db_schema();
        let table = if let Some(db) = &self.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };
        let col_values = record
            .db_values()
            .iter()
            .filter_map(|(c, v)| {
                if cols.is_empty() || cols.contains(c) {
                    Some(format!("{} = {}", c, v.to_sql_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("ALTER TABLE {} UPDATE {}{}", table, col_values, where_cond);

        let _res_bytes = self.send_query(query.into()).await?;
        Ok(())
    }

    /// Deletes a record
    #[tracing::instrument(skip(self))]
    pub async fn delete<T>(&self, where_cond: Where) -> Result<(), Error>
    where
        T: DbRecordExt,
    {
        let schema = T::db_schema();
        let table = if let Some(db) = &self.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };
        let query = format!("ALTER TABLE {} DELETE{}", table, where_cond);

        let _res_bytes = self.send_query(query.into()).await?;
        Ok(())
    }
}

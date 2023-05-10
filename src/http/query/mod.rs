//! HTTP operations
//!
//! # Examples
//!
//! GET query with param:
//! ```sh
//! curl 'http://localhost:8123/?query=SELECT%201'
//! ```
//!
//! GET query wit body
//! ```sh
//! echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
//! ```
//!
//! POST query with param:
//! ```sh
//! echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
//! echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
//! $ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
//! echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
//! ```
//!
//! # Settings
//!
//! ## HTTP compression
//!
//! Clickhouse supports HTTP compression. The request can pass the `Accept-Encoding` header. Valid values are:
//! gzip, br, deflate, xz, zstd, lz4, bz2, snappy.
//!
//! To compress the response, the CH setting `enable_http_compression`, and the header `Accept-Encoding` must be set.
//!
//! ## Database
//!
//! The database name can be passed in the URL (see below), or via a header
//!
//! ```sh
//! echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
//! ```
//!
//! ## User/password
//!
//! 3 ways to do it:
//! - HTTP basic auth: `echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-`
//! - URL params (NOT RECOMMENDED): `echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-`
//! - headers: `$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-`
//!
//! ## Other settings
//!
//! Other settings can be specified in the URL.

use std::collections::HashMap;

use hyper::Body;
use tracing::error;

use crate::{
    error::Error,
    schema::{DbRowExt, DbType, Schema, TableSchema},
};

use super::Client;

#[cfg(test)]
mod tests;

/// WHERE condition
#[derive(Debug, Clone)]
pub struct Where {
    /// Statement (prefix, column, condition, value)
    statements: Vec<(String, String, String, String)>,
}

impl Where {
    /// Instantiates with 1 condition
    pub fn new(col: &str, condition: &str, value: impl DbType) -> Self {
        let stmt: (String, String, String, String) = (
            "".to_string(),
            col.to_string(),
            condition.to_string(),
            value.to_sql_str(),
        );
        let statements = vec![stmt];
        Self { statements }
    }

    /// Instantiates with no condition
    pub fn null() -> Self {
        Self { statements: vec![] }
    }

    /// Adds an AND statement
    pub fn and(mut self, col: &str, condition: &str, value: impl DbType) -> Self {
        let stmt = (
            "AND".to_string(),
            col.to_string(),
            condition.to_string(),
            value.to_sql_str(),
        );
        self.statements.push(stmt);
        self
    }

    /// Adds an OR statement
    pub fn or(mut self, col: &str, condition: &str, value: impl DbType) -> Self {
        let stmt = (
            "OR".to_string(),
            col.to_string(),
            condition.to_string(),
            value.to_sql_str(),
        );
        self.statements.push(stmt);
        self
    }

    /// Checks if the condition is null
    pub fn is_null(&self) -> bool {
        self.statements.is_empty()
    }
}

impl std::fmt::Display for Where {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.is_null() {
            let s = self
                .statements
                .iter()
                .map(|(prefix, col, condition, value)| {
                    format!(
                        "{}({} {} {})",
                        if !prefix.is_empty() {
                            format!("{} ", prefix)
                        } else {
                            "".to_string()
                        },
                        col,
                        condition,
                        value.to_sql_str()
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            write!(f, " WHERE {s}")
        } else {
            write!(f, "")
        }
    }
}

impl Client {
    /// Creates a database
    #[tracing::instrument(skip(self))]
    pub async fn create_db(&self, schema: &Schema) -> Result<(), Error> {
        let query = format!(
            "CREATE DATABASE IF NOT EXISTS {} ENGINE = MergeTree()",
            schema.db_name
        );

        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Creates a table
    #[tracing::instrument(skip(self))]
    pub async fn create_table(&self, schema: &TableSchema) -> Result<(), Error> {
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
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree() PRIMARY KEY ({})",
            schema.name, fields, keys
        );

        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Inserts 1 or several records
    ///
    /// # Note
    ///
    /// In Clickhouse, there is no RETURNING statement
    #[tracing::instrument(skip_all, fields(records.len = records.len()))]
    pub async fn insert<T>(&self, records: &[T]) -> Result<(), Error>
    where
        T: DbRowExt,
    {
        let schema = T::db_schema();
        let table = schema.name;
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

        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Selects 1 or several records
    ///
    /// # Arguments
    ///
    /// - is cols is empty, all fields are retrieved
    #[tracing::instrument(skip(self))]
    pub async fn select<T>(&self, cols: &[&str], where_cond: &Where) -> Result<Vec<T>, Error>
    where
        T: DbRowExt + Default,
    {
        let schema = T::db_schema();
        let table = schema.name;
        let cols = if cols.is_empty() {
            "*".to_string()
        } else {
            cols.join(", ")
        };
        let query = format!("SELECT {cols} FROM {table}{where_cond} FORMAT TabSeparatedWithNames");

        let res_bytes = self.raw_query(query).await?;
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
    #[tracing::instrument(skip(self, record))]
    pub async fn update<T>(
        &self,
        record: &T,
        cols: &[&str],
        where_cond: &Where,
    ) -> Result<(), Error>
    where
        T: DbRowExt,
    {
        let schema = T::db_schema();
        let table = schema.name;
        let col_values = record
            .db_values()
            .iter()
            .filter_map(|(c, v)| {
                if cols.contains(c) {
                    Some(format!("{} = {}", c, v.to_sql_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("ALTER TABLE {} UPDATE {}{}", table, col_values, where_cond);

        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Deletes a record
    #[tracing::instrument(skip(self))]
    pub async fn delete<T>(&self, where_cond: &Where) -> Result<(), Error>
    where
        T: DbRowExt,
    {
        let schema = T::db_schema();
        let table = schema.name;
        let query = format!("ALTER TABLE {} DELETE{}", table, where_cond);

        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Sends a raw query
    ///
    /// # Arguments
    ///
    /// The argument is the raw query as a string
    ///
    /// # Result
    ///
    /// The result is a vector of bytes in case of success, or an error message in case of failure
    #[tracing::instrument(skip_all, fields(query = {let s: String = query.clone().into(); s }))]
    pub async fn raw_query(&self, query: impl Into<String> + Clone) -> Result<Vec<u8>, Error> {
        let query: String = query.into();

        let mut req_builder = hyper::Request::builder().uri(&self.url).method("POST");

        // add default database
        if let Some(db) = &self.db {
            const HEADER_DEFAULT_DB: &str = "X-ClickHouse-Database";
            req_builder = req_builder.header(HEADER_DEFAULT_DB, db);
        }

        // add credentials
        if let Some((username, password)) = &self.credentials {
            const HEADER_USER: &str = "X-ClickHouse-User";
            const HEADER_PASSWORD: &str = "X-ClickHouse-Key";
            req_builder = req_builder.header(HEADER_USER, username);
            req_builder = req_builder.header(HEADER_PASSWORD, password);
        }

        let body = Body::from(query);
        let req = req_builder.body(body)?;

        let res = self.http_client.request(req).await?;
        let res_status = res.status();
        let body_bytes = hyper::body::to_bytes(res.into_body()).await?;

        if res_status.is_success() {
            Ok(body_bytes.to_vec())
        } else {
            let res_body_str = String::from_utf8(body_bytes.to_vec())?;
            error!(error = res_body_str, "query failed");
            Err(Error(res_body_str))
        }
    }
}

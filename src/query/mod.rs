//! Queries

mod comp;
mod crud;
mod data;
mod exec;
mod fmt;
mod result;
mod sql;
mod stmt;

#[cfg(test)]
mod tests;

pub use comp::*;
pub use crud::*;
pub use data::*;
pub use exec::*;
pub use fmt::*;
pub use result::*;
pub use sql::*;
pub use stmt::*;

use crate::value::{ChValue, Value};

/// Query
///
/// A Query object is a complete representation of a query
#[derive(Default, Debug)]
pub struct Query {
    /// Statement (eg SELECT * FROM ...)
    pub statement: String,
    /// Data
    pub data: Option<QueryData>,
    /// Target DB
    pub db: Option<String>,
    /// Credentials (username, password)
    pub credentials: Option<(String, String)>,
    /// Format
    pub format: Option<Format>,
    /// Compress the request
    pub compress_request: Option<Compression>,
    /// Compress the HTTP response
    pub compress_response: Option<Compression>,
}

impl Query {
    /// Creates a new builder
    pub fn new(stmt: &str) -> Self {
        Query {
            statement: stmt.to_string(),
            data: None,
            db: None,
            credentials: None,
            format: None,
            compress_request: None,
            compress_response: None,
        }
    }

    /// Asssigns a statement
    pub fn statement(mut self, stmt: &str) -> Self {
        self.statement = stmt.to_string();
        self
    }

    /// Asssigns the query data
    pub fn data(mut self, table: QueryData) -> Self {
        self.data = Some(table);
        self
    }

    /// Binds the statement with a [ChValue]
    ///
    /// Query parameters are defined by `??`
    pub fn bind_val(mut self, value: impl ChValue) -> Self {
        self.statement = self.statement.bind_val(value);
        self
    }

    /// Binds the statement with a raw query value
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_str(mut self, value: &str) -> Self {
        self.statement = self.statement.bind_str(value);
        self
    }

    /// Binds the statement with a list of values
    pub fn bind_val_list(mut self, values: Vec<Value>) -> Self {
        self.statement = self.statement.bind_val_list(values);
        self
    }

    /// Binds the statement with a list of valeus as strings
    pub fn bind_str_list(mut self, values: Vec<&str>) -> Self {
        self.statement = self.statement.bind_str_list(values);
        self
    }

    /// Assigns a target DB
    pub fn db(mut self, db: &str) -> Self {
        self.db = Some(db.to_string());
        self
    }

    /// Assigns the credentials
    pub fn credentials(mut self, username: &str, password: &str) -> Self {
        self.credentials = Some((username.to_string(), password.to_string()));
        self
    }

    /// Assigns a format
    ///
    /// Eg. RowBinary
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Compress the HTTP request
    ///
    /// Eg. RowBinary
    pub fn compress_request(mut self, compression: Compression) -> Self {
        self.compress_request = Some(compression);
        self
    }

    /// Compress the HTTP response
    pub fn compress_response(mut self, compression: Compression) -> Self {
        self.compress_response = Some(compression);
        self
    }
}

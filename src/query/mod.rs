//! Queries

use crate::{
    core::{fmt::sql::SqlFormatter, Type, Value},
    error::Error,
    fmt::Format,
    intf::Interface,
    Client,
};

mod opts;

#[cfg(test)]
mod tests;

pub use opts::*;

/// Query
///
/// A Query object is a complete representation of a query
#[derive(Clone, Default)]
pub struct Query {
    /// Statement (eg SELECT * FROM ...)
    pub statement: String,
    /// Payload data
    pub data: Option<Vec<u8>>,
    /// Skip client
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
    /// Format
    pub format: Option<Format>,
    /// Compress the request
    pub compress_request: Option<Compression>,
    /// Compress the HTTP response
    pub compress_response: Option<Compression>,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("statement", &self.statement)
            .field("data", &self.data)
            .field("db", &self.db)
            .field("format", &self.format)
            .field("compress_request", &self.compress_request)
            .field("compress_response", &self.compress_response)
            .finish()
    }
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

    /// Assigns the target DB
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

    /// Compressed the HTTP request
    ///
    /// Eg. RowBinary
    pub fn compress(mut self, compression: Compression) -> Self {
        self.compress_request = Some(compression);
        self.compress_response = Some(compression);
        self
    }

    /// Binds the raw statement with a query value
    ///
    /// Query parameters are defined by `??`
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        let value: Value = value.into();
        let formatter = SqlFormatter::new();
        let value_str = value.format(&formatter);
        self = self.replace_bind_symbol(&value_str);
        self
    }

    /// Binds the query with a raw parameter which is not formatted
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_raw(self, value: &str) -> Self {
        self.replace_bind_symbol(value)
    }

    /// Replaces the bind symbol `??`
    fn replace_bind_symbol(mut self, value: &str) -> Self {
        const QUERY_PARAM_KEY: &str = "??";
        self.statement = self.statement.replacen(QUERY_PARAM_KEY, value, 1);
        self
    }
}

/// Query response
#[derive(Debug, Clone)]
pub struct QueryResponse {
    /// Data in bytes
    pub data: Vec<u8>,
}

impl QueryResponse {
    /// Creates a query response
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { data: bytes }
    }
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a query from a client
    pub fn query(&self, query: &str) -> QueryExecutor<T> {
        let mut query = Query::new(query);
        query.db = self.db.clone();
        query.credentials = self.credentials.clone();

        QueryExecutor {
            query,
            interface: &self.interface,
        }
    }
}

/// Query executor
pub struct QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Query
    query: Query,
    /// Interface
    interface: &'a T,
}

impl<'a, T> QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Binds the raw query with query parameters
    ///
    /// Query parameters are defined by `??`
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        self.query = self.query.bind(value);
        self
    }

    /// Binds the query with a raw parameter which is not formatted
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_raw(mut self, value: &str) -> Self {
        self.query = self.query.bind_raw(value);
        self
    }

    /// Assigns the target DB
    pub fn db(mut self, db: Option<&str>) -> Self {
        self.query.db = db.map(|d| d.to_string());
        self
    }

    /// Executes the query
    #[tracing::instrument(skip(self))]
    pub async fn exec(self) -> Result<QueryResponse, Error> {
        self.interface.send(self.query).await
    }
}

impl<'a, T> std::fmt::Debug for QueryExecutor<'a, T>
where
    T: Interface + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryExecutor")
            .field("raw_query", &self.query.statement)
            .field("interface", &self.interface)
            .finish()
    }
}

/// Query table
///
/// A query view represents a view of the data.
#[derive(Debug, Default)]
pub struct QueryTable {
    /// Column names
    pub names: Vec<String>,
    /// Column types
    pub types: Vec<Type>,
    /// Rows
    ///
    /// The 1st Vec is for rows, the 2nd for each row column
    pub rows: Vec<Vec<Value>>,
}

impl QueryTable {
    /// Returns the number of rows
    pub fn nb_rows(&self) -> usize {
        self.rows.len()
    }
}

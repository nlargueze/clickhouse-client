//! Queries

use crate::{error::Error, interface::Interface, orm::Value, Client};

mod ddl;
mod fmt;

#[cfg(test)]
mod tests;

use fmt::sql::SqlSerializer;
pub use fmt::*;
use serde::Serialize;

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a query
    pub fn query(&self, query: &str) -> QueryExecutor<T> {
        QueryExecutor {
            raw_query: query.to_string(),
            client: self,
        }
    }
}

/// Query executor
pub struct QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Raw query
    raw_query: String,
    /// Client
    client: &'a Client<T>,
}

impl<'a, T> QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Binds the raw query with query parameters
    ///
    /// Query parameters are defined by `??`
    pub fn bind(mut self, value: impl Into<Value>) -> Self {
        let sql_serializer = SqlSerializer::new();
        let value: Value = value.into();
        let value_str = value
            .serialize(sql_serializer)
            .expect("cannot serialize value to SQL");
        self.replace_bind_symbol(&value_str);
        self
    }

    /// Binds the raw query with raw query parameters
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_raw(mut self, value: &str) -> Self {
        self.replace_bind_symbol(value);
        self
    }

    /// Executes the query
    #[tracing::instrument(skip(self))]
    pub async fn exec(self) -> Result<Vec<u8>, Error> {
        self.client
            .interface
            .raw_query(&self.raw_query, self.client.raw_query_opts())
            .await
    }

    /// Replaces the bind symbol
    fn replace_bind_symbol(&mut self, value: &str) {
        const QUERY_PARAM_KEY: &str = "??";
        self.raw_query = self.raw_query.replacen(QUERY_PARAM_KEY, value, 1);
    }
}

impl<'a, T> std::fmt::Debug for QueryExecutor<'a, T>
where
    T: Interface + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryExecutor")
            .field("raw_query", &self.raw_query)
            .field("raw_query_options", &self.client.raw_query_opts())
            .field("interface", &self.client)
            .finish()
    }
}

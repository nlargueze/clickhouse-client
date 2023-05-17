//! Query executor

#[cfg(test)]
mod tests;

use crate::{
    error::Error,
    intf::Interface,
    value::{ChValue, Value},
    Client,
};

use super::{Format, Query, QueryData, QueryResponse, SqlStatement};

/// Query executor
#[derive(Debug)]
pub struct QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Client
    client: &'a Client<T>,
    /// Query
    query: Query,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a new query
    pub fn query(&self, query: &str) -> QueryExecutor<T> {
        QueryExecutor {
            client: self,
            query: Query {
                statement: query.to_string(),
                db: self.db.clone(),
                credentials: self.credentials.clone(),
                ..Default::default()
            },
        }
    }
}

impl<'a, T> QueryExecutor<'a, T>
where
    T: Interface,
{
    /// Binds the raw query with query parameters
    ///
    /// Query parameters are defined by `??`
    pub fn bind_val(mut self, value: impl ChValue) -> Self {
        self.query.statement = self.query.statement.bind_val(value);
        self
    }

    /// Binds the query with a raw parameter which is not formatted
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_str(mut self, value: &str) -> Self {
        self.query.statement = self.query.statement.bind_str(value);
        self
    }

    /// Binds the raw query with query parameters
    pub fn bind_val_list(mut self, values: Vec<Value>) -> Self {
        self.query.statement = self.query.statement.bind_val_list(values);
        self
    }

    /// Binds the query with strings
    pub fn bind_str_list(mut self, values: Vec<&str>) -> Self {
        self.query.statement = self.query.statement.bind_str_list(values);
        self
    }

    /// Asssigns the query data
    pub fn data(mut self, table: QueryData) -> Self {
        self.query.data = Some(table);
        self
    }

    /// Assigns the target DB
    pub fn db(mut self, db: Option<&str>) -> Self {
        self.query.db = db.map(|d| d.to_string());
        self
    }

    /// Assigns the format
    pub fn format(mut self, format: Format) -> Self {
        self.query.format = Some(format);
        self
    }

    /// Executes the query
    #[tracing::instrument(skip(self))]
    pub async fn exec(self) -> Result<QueryResponse, Error> {
        self.client.send(self.query).await
    }
}

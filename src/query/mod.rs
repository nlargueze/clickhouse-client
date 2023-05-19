//! Queries

use crate::{error::Error, interface::Interface, Client};

pub mod ddl;
pub mod format;
// mod orm;

pub use format::*;
pub use sql::*;

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
    const QUERY_PARAM_KEY: &str = "??";

    /// Binds the raw query with query parameters
    ///
    /// Query parameters are defined by `??`
    pub fn bind(mut self, value: impl ToSqlString) -> Self {
        self.raw_query =
            self.raw_query
                .replacen(Self::QUERY_PARAM_KEY, value.to_sql_string().as_str(), 1);
        self
    }

    /// Binds the raw query with raw query parameters
    ///
    /// For instance, strings are not enclosed by `'`.
    ///
    /// Query parameters are defined by `??`
    pub fn bind_raw(mut self, value: &str) -> Self {
        self.raw_query = self.raw_query.replacen(Self::QUERY_PARAM_KEY, value, 1);
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

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_query_simple() {
        let client = crate::tests::init().await;
        client
            .query("SELECT * FROM tests WHERE uuid = ??")
            .bind("6f2f0129-7956-4d73-80b8-1860fbe1121a")
            .exec()
            .await
            .unwrap();
    }
}

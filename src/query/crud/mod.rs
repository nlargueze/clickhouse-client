//! Query CRUD executor

#[cfg(test)]
mod tests;

use crate::{error::Error, intf::Interface, value::Value, Client};

use super::{Format, Query, QueryData, QueryResponse, Where};

/// CRUD query
#[derive(Debug)]
pub struct CRUDQuery<'a, T>
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
    /// Prepares a [CRUDQuery] from a client
    pub fn crud(&self) -> CRUDQuery<T> {
        CRUDQuery {
            client: self,
            query: Query {
                db: self.db.clone(),
                credentials: self.credentials.clone(),
                ..Default::default()
            },
        }
    }
}

impl<'a, T> CRUDQuery<'a, T>
where
    T: Interface,
{
    /// Sets the query format
    pub fn format(mut self, format: Format) -> Self {
        self.query.format = Some(format);
        self
    }

    /// Insert rows(s)
    #[tracing::instrument(skip(self, data))]
    pub async fn insert(self, table: &str, data: QueryData) -> Result<QueryResponse, Error> {
        // NB: To pass the data inside the HTTP body, a FORMAT clause must be passed to the
        // SQL statement explicitly.
        let format = self.query.format.unwrap_or(Format::RowBinary);

        let query = self
            .query
            .statement("INSERT INTO [??] FORMAT [??]")
            .bind_str(table)
            .bind_str(&format.to_string())
            .format(format)
            .data(data);
        self.client.send(query).await
    }

    /// Select rows
    ///
    /// If the columns is empty, all columns are returned
    #[tracing::instrument(skip(self))]
    pub async fn select(
        self,
        table: &str,
        fields: Vec<&str>,
        where_cond: Option<Where>,
    ) -> Result<QueryResponse, Error> {
        let fields = if fields.is_empty() {
            "*".to_string()
        } else {
            fields
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let query = self
            .query
            .statement("SELECT [??] FROM [??][??]")
            .bind_str(&fields)
            .bind_str(table)
            .bind_str(where_cond.unwrap_or_default().to_string().as_str());
        self.client.send(query).await
    }

    /// Update rows
    #[tracing::instrument(skip(self))]
    pub async fn update(
        self,
        table: &str,
        fields: Vec<(&str, Value)>,
        where_cond: Option<Where>,
    ) -> Result<QueryResponse, Error> {
        let fields = fields
            .iter()
            .map(|(k, v)| format!("{} = {}", k, v.clone().to_sql_string()))
            .collect::<Vec<_>>()
            .join(", ");

        let query = self
            .query
            .statement("ALTER TABLE [??] UPDATE [??][??]")
            .bind_str(table)
            .bind_str(&fields)
            .bind_str(&where_cond.unwrap_or_default().to_string());
        self.client.send(query).await
    }

    /// Delete rows
    ///
    /// If the columns is empty, all columns are returned
    #[tracing::instrument(skip(self))]
    pub async fn delete(
        self,
        table: &str,
        where_cond: Option<Where>,
    ) -> Result<QueryResponse, Error> {
        let query = self
            .query
            .statement("DELETE FROM [??][??]")
            .bind_str(table)
            .bind_str(&where_cond.unwrap_or_default().to_string());
        self.client.send(query).await
    }
}

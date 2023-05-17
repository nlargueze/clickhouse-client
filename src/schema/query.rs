//! DDL queries

use crate::{error::Error, intf::Interface, query::Query, Client};

use super::TableSchema;

/// DDL query
pub struct DdlQuery<'a, T>
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
    /// Prepares a DDL query
    pub fn ddl(&self) -> DdlQuery<T> {
        DdlQuery {
            client: self,
            query: Query {
                db: self.db.clone(),
                credentials: self.credentials.clone(),
                ..Default::default()
            },
        }
    }
}

impl<'a, T> DdlQuery<'a, T>
where
    T: Interface,
{
    /// Creates a DB
    #[tracing::instrument(skip(self))]
    pub async fn create_db(self, db: &str) -> Result<(), Error> {
        let mut query = self
            .query
            .statement("CREATE DATABASE IF NOT EXISTS [??]")
            .bind_str(db);
        query.db = None;
        self.client.send(query).await?;
        Ok(())
    }

    /// Drops a DB
    #[tracing::instrument(skip(self))]
    pub async fn drop_db(self, db: &str) -> Result<(), Error> {
        let mut query = self
            .query
            .statement("DROP DATABASE IF NOT EXISTS [??]")
            .bind_str(db);
        query.db = None;
        self.client.send(query).await?;
        Ok(())
    }

    /// Creates a table
    #[tracing::instrument(skip(self))]
    pub async fn create_table(self, schema: &TableSchema, engine: &str) -> Result<(), Error> {
        let columns = schema
            .columns
            .iter()
            .map(|col| format!("{} {}", col.id, col.ty))
            .collect::<Vec<_>>();

        let primary_keys = schema
            .columns
            .iter()
            .filter_map(|col| {
                if col.primary {
                    Some(col.id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let query = self
            .query
            .statement("CREATE TABLE IF NOT EXISTS [??] ([??]) ENGINE = [??] PRIMARY KEY ([??])")
            .bind_str(&schema.name)
            .bind_str(&columns.join(", "))
            .bind_str(engine)
            .bind_str(&primary_keys.join(", "));
        self.client.send(query).await?;
        Ok(())
    }

    /// Drops a table
    #[tracing::instrument(skip(self))]
    pub async fn drop_table(self, table: &str) -> Result<(), Error> {
        let query = self
            .query
            .statement("DROP TABLE IF EXISTS [??]")
            .bind_str(table);
        self.client.send(query).await?;
        Ok(())
    }
}

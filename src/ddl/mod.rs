//! DDL

mod sch;

pub use sch::*;

use crate::{error::Error, intf::Interface, Client};

/// DDL query
pub struct DdlQuery<'a, T>
where
    T: Interface,
{
    /// Client
    client: &'a Client<T>,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a DDL query
    pub fn ddl(&self) -> DdlQuery<T> {
        DdlQuery { client: self }
    }
}
impl<'a, T> DdlQuery<'a, T>
where
    T: Interface,
{
    /// Creates a DB
    #[tracing::instrument(skip(self))]
    pub async fn create_db(&self, db: &str) -> Result<(), Error> {
        self.client
            .query("CREATE DATABASE IF NOT EXISTS ??")
            .bind_raw(db)
            .db(None)
            .exec()
            .await?;
        Ok(())
    }

    /// Creates a table
    #[tracing::instrument(skip(self))]
    pub async fn create_table(&self, schema: &TableSchema, engine: &str) -> Result<(), Error> {
        let table = if let Some(db) = &self.client.db {
            format!("{}.{}", db, schema.name)
        } else {
            schema.name.to_string()
        };

        let columns = schema
            .columns
            .iter()
            .map(|col| format!("{} {}", col.id, col.ty))
            .collect::<Vec<_>>()
            .join(", ");

        let primary_keys = schema
            .columns
            .iter()
            .filter_map(|col| {
                if col.is_primary {
                    Some(col.id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        self.client
            .query("CREATE TABLE IF NOT EXISTS ?? (??) ENGINE = ?? PRIMARY KEY (??)")
            .bind_raw(&table)
            .bind_raw(&columns)
            .bind_raw(engine)
            .bind_raw(&primary_keys)
            .exec()
            .await?;
        Ok(())
    }

    /// Drops a table
    #[tracing::instrument(skip(self))]
    pub async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let table = if let Some(db) = &self.client.db {
            format!("{}.{}", db, table)
        } else {
            table.to_string()
        };

        self.client
            .query("DROP TABLE IF EXISTS ??")
            .bind_raw(&table)
            .exec()
            .await?;
        Ok(())
    }
}

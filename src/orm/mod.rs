//! ORM

mod query;

pub use query::*;

#[cfg(test)]
mod tests;

/// ORM prelude
pub mod prelude {
    pub use super::OrmExt;
    pub use crate::{
        core::{TypeOrm, Value},
        ddl::{ColSchema, TableSchema},
        error::Error,
    };
    pub use clickhouse_client_macros::Orm;
    pub use once_cell;
}

use crate::{
    core::{Type, Value},
    ddl::TableSchema,
    error::Error,
    intf::Interface,
    Client,
};
use std::{collections::HashMap, marker::PhantomData};

/// Extension trait to represent a Rust struct as a database record
pub trait OrmExt {
    /// Returns the DB table schema
    fn db_schema() -> &'static TableSchema;

    /// Returns the DB values
    fn db_values(&self) -> HashMap<String, Value>;

    /// Parses from DB values
    fn from_db_values(values: &HashMap<String, Value>) -> Result<Self, Error>
    where
        Self: Default;

    /// Returns the column names
    fn db_names(&self) -> Vec<String> {
        Self::db_schema()
            .columns
            .iter()
            .map(|col| col.id.clone())
            .collect()
    }

    /// Returns the column types
    fn db_types(&self) -> Vec<Type> {
        Self::db_schema()
            .columns
            .iter()
            .map(|col| col.ty.clone())
            .collect()
    }

    /// Returns the row
    fn db_row(&self) -> Vec<Value> {
        let names = self.db_names();
        let values = self.db_values();
        names
            .iter()
            .map(|name| values.get(name).expect("invalid column name").clone())
            .collect()
    }
}

// /// Extension trait to represent a Rust type as a
// pub trait OrmFieldExt: Into<Value> + From<Value> {
//     //
// }

/// ORM query
pub struct OrmQuery<'a, T, U>
where
    T: Interface,
    U: OrmExt,
{
    /// Client
    client: &'a Client<T>,
    /// Record
    record: PhantomData<U>,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a query for a specific record type
    pub fn orm<U>(&self) -> OrmQuery<T, U>
    where
        U: OrmExt,
    {
        OrmQuery {
            client: self,
            record: PhantomData,
        }
    }
}

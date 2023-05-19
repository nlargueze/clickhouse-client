//! ORM mapping
//!
//! The module provides mapping between Rust types and Clickhouse types,
//! and between Rust structs and Clickhouse row records.

mod query;

#[cfg(test)]
mod tests;

/// ORM prelude
pub mod prelude {
    pub use super::{DbRecord, DbRecordExt};
    pub use crate::schema::{ColSchema, TableSchema};
    pub use phf;
}

use crate::{interface::Interface, schema::TableSchema, Client};
use std::marker::PhantomData;

pub use clickhouse_client_macros::DbRecord;

/// Extension trait to represent a Rust struct as a database record
pub trait DbRecordExt {
    /// Table chema
    const DB_TABLE: TableSchema;
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a query for a specific record type
    pub fn orm<U>(&self) -> OrmQueryExecutor<T, U>
    where
        U: DbRecordExt,
    {
        OrmQueryExecutor {
            client: self,
            record: PhantomData,
        }
    }
}

/// Query executor
pub struct OrmQueryExecutor<'a, T, U>
where
    T: Interface,
    U: DbRecordExt,
{
    /// Client
    client: &'a Client<T>,
    /// Record
    record: PhantomData<U>,
}

//! ORM

mod query;

pub use query::*;

#[cfg(test)]
mod tests;

/// ORM prelude
pub mod prelude {
    pub use super::OrmExt;
    pub use crate::core::{ColSchema, TableSchema};
    pub use clickhouse_client_macros::Orm;
}

use crate::{interface::Interface, Client};
use std::marker::PhantomData;

use prelude::*;

fn global_data() -> &'static Vec<u8> {
    static INSTANCE: once_cell::sync::OnceCell<Vec<u8>> = once_cell::sync::OnceCell::new();
    INSTANCE.get_or_init(|| {
        let mut v = Vec::new();
        v.push(1);
        v
    })
}
// /// Test record
// #[derive(Debug, Orm)]
// #[db(table = "records")]
// struct TestRecord {
//     /// ID
//     #[db(primary_key)]
//     id: u8,
//     /// Name
//     name: String,
// }

/// Extension trait to represent a Rust struct as a database record
pub trait OrmExt {
    /// Returns the DB table schema
    fn db_schema() -> TableSchema;
}

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

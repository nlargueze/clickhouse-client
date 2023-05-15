//! DB row
//!
//! This module provides a [DbRowExt] which maps a Rust type to and from a DB record.
//!
//! A [DbRow] macro is provided to derive that trait for structs.

use std::collections::HashMap;

pub use clickhouse_client_macros::DbRow;

use super::{DbValue, TableSchema};

/// Extension trait to represent a Rust type as a database row
pub trait DbRowExt {
    /// Returns the table schema
    fn db_schema() -> TableSchema;

    /// Returns the DB values
    fn db_values(&self) -> HashMap<&'static str, Box<&'_ dyn DbValue>>;

    /// Parses the row from a map(column, value)
    fn from_db_values(values: HashMap<&str, &str>) -> Result<Self, String>
    where
        Self: Sized + Default;
}

#[cfg(test)]
mod tests {
    use crate::schema::prelude::*;
    use time::OffsetDateTime;
    use tracing::info;

    use super::*;

    fn init() {
        crate::tests::init_test_tracer();
    }

    /// A sample struct that represents a DB record
    #[derive(Debug, DbRow)]
    #[db(table = "test_derive")]
    struct TestRow {
        /// ID
        #[db(primary)]
        id: u8,
        /// Name
        #[db(primary, name = "name2")]
        name: String,
        /// Timestamp
        dt: OffsetDateTime,
    }

    impl Default for TestRow {
        fn default() -> Self {
            Self {
                id: Default::default(),
                name: Default::default(),
                dt: OffsetDateTime::UNIX_EPOCH,
            }
        }
    }

    #[test]
    fn test_derive_simple() {
        init();

        let row = TestRow {
            id: 1,
            name: "nick".to_string(),
            dt: OffsetDateTime::now_utc(),
        };
        let _row_values = row.db_values();
        info!("TestRow OK");
        eprintln!("{_row_values:#?}");
    }
}

//! DB record
//!
//! This module provides a [Record] trait which must be implemented by structs representing
//! database records.

mod types;

use std::collections::HashMap;

pub use clickhouse_client_macros::DbRow;
pub use types::*;

/// Trait to represent a database row
pub trait IsDbRow {
    /// Returns the table name
    fn table() -> &'static str;

    /// Returns the list of columns
    fn columns() -> Vec<&'static str>;

    /// Returns the row values
    fn values(&self) -> HashMap<&'static str, Box<dyn DbType + '_>>;
}

// NOTES
//
// - a arbitraty struct mut implement the trait [Record] to pass to/from a query
// - a struct which implements the [Record] trait can return a map of field name => field value as clickhouse data type
// - a rust type is mapped to a specific Clickhouse data type.
// - a Clickhouse data type can be converted to a string passed to a query.
//
// In the other direction
// - when receiving bytes from the DB, a field can be mapped from (String, bytes) => Clickhouse data type
// - a Clickhouse data type can be converted back to the struct field type.

#[cfg(test)]
mod tests {

    use time::OffsetDateTime;

    use super::*;

    /// A sample struct that represents a DB record
    #[derive(Debug, DbRow)]
    #[db(table = "table_name")]
    struct TestRow {
        /// ID
        // #[db(name = "primary_key")]
        id: u8,
        /// Name
        name: String,
        /// Timestamp
        dt: OffsetDateTime,
    }

    #[test]
    fn test_derive_row() {
        let row = TestRow {
            id: 1,
            name: "nick".to_string(),
            dt: OffsetDateTime::now_utc(),
        };
        let row_values = row.values();
        eprintln!("{row_values:#?}");
    }
}

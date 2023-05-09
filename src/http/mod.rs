//! HTTP interface
//!
//! This module provides a HTTP [Client] for the ClickHouse database.
//!
//! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).

mod client;
mod query;

pub use client::*;
pub use query::*;

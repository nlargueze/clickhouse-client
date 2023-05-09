//! HTTP interface
//!
//! This module provides a HTTP [Client] for the ClickHouse database.
//!
//! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).
//!
//! # Overview
//!
//! - Port 8123 by default (8443 for HTTPS)
//! - Healthcheck: `curl http://localhost:8123/ping`
//! - Playground at `http://localhost:8123/play`
//! - query can be sent with a GET + query param `?query=`, or with a POST

mod client;
mod query;

pub use client::*;
pub use query::*;

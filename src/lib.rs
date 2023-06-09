//! Clickhouse client
//!
//! This crate provides a Clickhouse client.
//!
//! - HTTP interface
//! - Query builder
//! - ORM to map to Rust types
//!
//! # Features
//!
//! - **time**: support for the `time` crate types
//! - **uuid**: support for the `uuid` crate types

#![deny(missing_docs)]

use intf::{http::Http, Interface};

pub mod core;
pub mod ddl;
pub mod error;
pub mod fmt;
pub mod intf;
pub mod orm;
pub mod query;

/// Clickhouse client
pub struct Client<T>
where
    T: Interface,
{
    /// Database
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
    /// Interface
    pub interface: T,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Sets the target database
    pub fn database(mut self, db: &str) -> Self {
        self.db = Some(db.to_string());
        self
    }

    /// Adds the credentials
    pub fn credentials(mut self, username: &str, password: &str) -> Self {
        self.credentials = Some((username.to_string(), password.to_string()));
        self
    }
}

impl Default for Client<Http> {
    fn default() -> Client<Http> {
        let interface = Http::new("http://localhost:8123");
        Self {
            db: None,
            credentials: Default::default(),
            interface,
        }
    }
}

impl<T> std::fmt::Debug for Client<T>
where
    T: Interface + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("db", &self.db)
            .field("credentials", &self.credentials)
            .field("interface", &self.interface)
            .finish()
    }
}

impl<T> Clone for Client<T>
where
    T: Interface + Clone,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            credentials: self.credentials.clone(),
            interface: self.interface.clone(),
        }
    }
}

/// Client with the HTTP interface
pub type HttpClient = Client<Http>;

#[cfg(test)]
mod tests {
    use crate::{core::Type, ddl::TableSchema, Client, HttpClient};
    use std::sync::Once;
    use tokio::sync::OnceCell;
    use tracing_ext::sub::PrettyConsoleLayer;
    use tracing_subscriber::{prelude::*, EnvFilter};

    static INIT_TRACER: Once = Once::new();

    static INIT_DB: OnceCell<()> = OnceCell::const_new();

    /// Initializes a client (and a tracer, and a test table)
    pub(crate) async fn init() -> HttpClient {
        INIT_TRACER.call_once(|| {
            let layer_pretty_stdout = PrettyConsoleLayer::default()
                .wrapped(true)
                .oneline(false)
                .events_only(false)
                .show_time(false)
                .show_target(true)
                .show_file_info(true)
                .show_span_info(true)
                .indent(6);
            let filter_layer = EnvFilter::from_default_env();

            tracing_subscriber::registry()
                .with(layer_pretty_stdout)
                .with(filter_layer)
                .init();
        });

        INIT_DB
            .get_or_init(|| async {
                let client = Client::default().database("test");
                client.ddl().create_db("test").await.unwrap();
                let schema = TableSchema::new("tests")
                    .new_column("uuid", Type::UUID, true)
                    .new_column("string", Type::String, false)
                    .new_column("uint8", Type::UInt8, false)
                    .new_column("date", Type::Date, false)
                    .new_column("date32", Type::Date32, false)
                    .new_column("datetime", Type::DateTime, false)
                    .new_column("datetime64", Type::DateTime64(9), false);
                client.ddl().drop_table("tests").await.unwrap();
                client
                    .ddl()
                    .create_table(&schema, "MergeTree()")
                    .await
                    .unwrap();
                client
                    .query(
                        "\
                INSERT INTO tests (uuid, string, uint8, date, date32, datetime, datetime64) \
                VALUES (\
                '63712f62-a87a-4d0f-9673-a17380428dc4', \
                'john', \
                1, \
                '1970-01-01', \
                '1971-01-01', \
                '1972-01-01 00:00:00', \
                '1973-01-01 00:00:00.0'\
                ) \
                ",
                    )
                    .exec()
                    .await
                    .unwrap();
            })
            .await;

        Client::default().database("test")
    }
}

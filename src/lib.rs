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

use interface::{http::Http, Interface};

pub mod error;
pub mod interface;
pub mod orm;
pub mod query;
pub mod schema;

/// Clickhouse client
pub struct Client {
    /// Database
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
    /// Interface
    pub interface: Box<dyn Interface>,
}

impl Client {
    /// Creates a new client (HTTP interface by default)
    pub fn new(url: &str) -> Self {
        let interface = Box::new(Http::new(url));
        Self {
            db: None,
            credentials: None,
            interface,
        }
    }

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

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use tracing_ext::sub::PrettyConsoleLayer;
    use tracing_subscriber::{prelude::*, EnvFilter};

    static INIT: Once = Once::new();

    /// Initializes a tracer for unit tests
    pub(crate) fn init_tracer() {
        INIT.call_once(|| {
            let layer_pretty_stdout = PrettyConsoleLayer::default()
                .wrapped(true)
                .oneline(false)
                .events_only(false)
                .show_time(false)
                .show_target(true)
                .show_span_info(true)
                .indent(6);
            let filter_layer = EnvFilter::from_default_env();

            tracing_subscriber::registry()
                .with(layer_pretty_stdout)
                .with(filter_layer)
                .init();
        });
    }
}

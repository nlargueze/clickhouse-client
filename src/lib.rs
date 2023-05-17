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

#[cfg(test)]
mod tests;

use intf::{http::Http, Interface};

pub mod error;
pub mod intf;
pub mod orm;
pub mod query;
pub mod schema;
pub mod value;

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

/// Client with the HTTP interface
pub type HttpClient = Client<Http>;

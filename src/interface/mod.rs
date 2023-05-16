//! DB interface
//!
//! The interface defines the interface used to communicate with the DB

use async_trait::async_trait;

use crate::{error::Error, query::Query, Client};

pub mod http;

/// An interface is a means of communicating with the database
#[async_trait]
pub trait Interface: Send + Sync {
    /// Sends a ping request
    async fn ping(&self) -> bool;

    /// Sends a query, and returns results as string
    async fn send_raw_query(
        &self,
        query: &str,
        options: SendRawQueryOptions,
    ) -> Result<Vec<u8>, Error>;
}

/// Options for the `send_raw_query` interface method
#[derive(Debug, Default)]
pub struct SendRawQueryOptions {
    /// DB
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
}

impl SendRawQueryOptions {
    /// Creates a new SendRawQueryOptions
    pub fn new() -> Self {
        Self::default()
    }

    /// Assigns a DB to the request
    pub fn db(mut self, db: &str) -> Self {
        self.db = Some(db.to_string());
        self
    }

    /// Assigns credentials to the request
    pub fn credentials(mut self, username: &str, password: &str) -> Self {
        self.credentials = Some((username.to_string(), password.to_string()));
        self
    }
}

impl Client {
    /// Pings the DB
    pub async fn ping(&self) -> bool {
        self.interface.ping().await
    }

    /// Sends a query
    pub async fn send_query(&self, query: Query) -> Result<Vec<u8>, Error> {
        let options = self.send_raw_query_opts();
        self.interface
            .send_raw_query(query.to_string().as_str(), options)
            .await
    }

    /// Returns the raw query options from the client
    pub(crate) fn send_raw_query_opts(&self) -> SendRawQueryOptions {
        SendRawQueryOptions {
            db: self.db.clone(),
            credentials: self.credentials.clone(),
        }
    }
}

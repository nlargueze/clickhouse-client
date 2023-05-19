//! DB interface
//!
//! The interface defines the interface used to communicate with the DB

use async_trait::async_trait;

use crate::{error::Error, Client};

pub mod http;

/// Options for the `raw_query` interface method
#[derive(Default)]
pub struct RawQueryOptions {
    /// DB
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
}

impl RawQueryOptions {
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

// NB: we need to implement this manually to exclude credentials
impl std::fmt::Debug for RawQueryOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendRawQueryOptions")
            .field("db", &self.db)
            .finish()
    }
}

/// An interface is a means of communicating with the database
#[async_trait]
pub trait Interface: Send + Sync {
    /// Sends a query, and returns results as string
    async fn raw_query(&self, query: &str, options: RawQueryOptions) -> Result<Vec<u8>, Error>;

    /// Sends a ping request
    async fn ping(&self) -> bool;
}

impl<F> Client<F>
where
    F: Interface,
{
    /// Pings the DB
    #[tracing::instrument(skip_all)]
    pub async fn ping(&self) -> bool {
        self.interface.ping().await
    }

    /// Creates a database
    #[tracing::instrument(skip(self))]
    pub async fn create_db(&self, db: &str) -> Result<(), Error> {
        let query = format!("CREATE DATABASE IF NOT EXISTS {}", db);
        let mut opts = self.raw_query_opts();
        opts.db = None;
        let _res_bytes = self.interface.raw_query(&query, opts).await?;
        Ok(())
    }

    /// Returns the raw query options from the client
    pub(crate) fn raw_query_opts(&self) -> RawQueryOptions {
        RawQueryOptions {
            db: self.db.clone(),
            credentials: self.credentials.clone(),
        }
    }
}

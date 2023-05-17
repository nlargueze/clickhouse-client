//! Interface
//!
//! The interface defines the interface used to communicate with the DB

use async_trait::async_trait;

use crate::{
    error::Error,
    query::{Query, QueryResponse},
    Client,
};

pub mod http;

/// An interface is a means of communicating with the database
#[async_trait]
pub trait Interface: Send + Sync {
    /// Sends a ping request
    async fn ping(&self) -> bool;

    /// Sends a query
    async fn send(&self, query: Query) -> Result<QueryResponse, Error>;
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Pings the DB
    #[tracing::instrument(skip_all)]
    pub async fn ping(&self) -> bool {
        self.interface.ping().await
    }

    /// Sends a query
    pub async fn send(&self, query: Query) -> Result<QueryResponse, Error> {
        self.interface.send(query).await
    }
}

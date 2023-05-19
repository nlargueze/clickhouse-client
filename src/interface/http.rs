//! HTTP interface
//!
//! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).

use async_trait::async_trait;
use hyper::{body::Bytes, Body, Request, Uri};

use crate::error::Error;

use super::{Interface, RawQueryOptions};

type HyperHttpsClient = hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>;

/// HTTP interface
#[derive(Debug)]
pub struct Http {
    /// URL
    url: Uri,
    /// HTTP client
    http_client: HyperHttpsClient,
}

impl Http {
    /// Creates a new HTTP interface
    pub fn new(url: &str) -> Self {
        let url: Uri = url.parse().unwrap();
        let https_conn = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let client = hyper::Client::<_, hyper::Body>::builder().build(https_conn);
        Self {
            url,
            http_client: client,
        }
    }
}

#[async_trait]
impl Interface for Http {
    #[tracing::instrument(skip(self))]
    async fn ping(&self) -> bool {
        let req = Request::builder()
            .uri(&self.url)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        match self.http_client.request(req).await {
            Ok(res) => res.status().is_success(),
            Err(_) => false,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn raw_query(&self, query: &str, options: RawQueryOptions) -> Result<Vec<u8>, Error> {
        let mut req_builder = hyper::Request::builder().uri(&self.url).method("POST");

        if let Some(db) = options.db {
            const HEADER_DEFAULT_DB: &str = "X-ClickHouse-Database";
            req_builder = req_builder.header(HEADER_DEFAULT_DB, db);
        }

        if let Some((username, password)) = options.credentials {
            const HEADER_USER: &str = "X-ClickHouse-User";
            const HEADER_PASSWORD: &str = "X-ClickHouse-Key";
            req_builder = req_builder.header(HEADER_USER, username);
            req_builder = req_builder.header(HEADER_PASSWORD, password);
        }

        let body = Body::from(Bytes::from(query.to_string()));
        let req = req_builder.body(body)?;

        let res = self.http_client.request(req).await?;
        let res_status = res.status();
        let body_bytes = hyper::body::to_bytes(res.into_body()).await?;

        if res_status.is_success() {
            Ok(body_bytes.to_vec())
        } else {
            let res_body_str = String::from_utf8(body_bytes.to_vec())?;
            tracing::error!(error = res_body_str, "query failed");
            Err(Error::new(res_body_str.as_str()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::interface::Interface;

    #[tokio::test]
    async fn test_http_ping() {
        let client = crate::tests::init().await;
        assert!(client.ping().await);
    }

    #[tokio::test]
    #[tracing::instrument]
    async fn test_http_raw_query() {
        let client = crate::tests::init().await;

        let raw_query = "SELECT 1";
        match client
            .interface
            .raw_query(raw_query, client.raw_query_opts())
            .await
        {
            Ok(ok) => {
                let res_body_str = String::from_utf8(ok).unwrap();
                tracing::info!(res_body_str, "test_http_raw_query OK");
            }
            Err(err) => {
                tracing::error!(%err, "test_http_raw_query ERROR");
                panic!("{err}")
            }
        }
    }
}

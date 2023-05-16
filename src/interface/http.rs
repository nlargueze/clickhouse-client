//! HTTP interface
//!
//! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).

use async_trait::async_trait;
use hyper::{Body, Request, Uri};

use crate::error::Error;

use super::{Interface, SendRawQueryOptions};

type HttpClient = hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>;

/// HTTP interface
pub struct Http {
    /// URL
    url: Uri,
    /// HTTP client
    http_client: HttpClient,
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

    async fn send_raw_query(
        &self,
        query: &str,
        options: SendRawQueryOptions,
    ) -> Result<Vec<u8>, Error> {
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

        let body = Body::from(query.to_string());
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
    use tokio::sync::OnceCell;

    use crate::Client;

    static INIT: OnceCell<Client> = OnceCell::const_new();

    async fn init() -> &'static Client {
        crate::tests::init_tracer();
        INIT.get_or_init(|| async { Client::new("http://localhost:8123").database("test") })
            .await
    }

    #[tokio::test]
    async fn test_http_ping() {
        let client = init().await;
        assert!(client.ping().await);
    }

    #[tokio::test]
    #[tracing::instrument]
    async fn test_http_raw_query() {
        let client = init().await;

        let raw_query = "SELECT 1";
        match client
            .interface
            .send_raw_query(raw_query, client.send_raw_query_opts())
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
        tracing::info!("test_http_raw_query OK");
    }
}

//! HTTP interface
//!
//! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).

#[cfg(test)]
mod tests;

use async_trait::async_trait;
use hyper::{Body, Request, Uri};
use tracing::{error, trace};

use crate::{
    error::Error,
    query::{Format, Query, QueryResponse},
};

use super::Interface;

type HyperHttpsClient = hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>;

/// HTTP interface
#[derive(Debug)]
pub struct Http {
    /// HTTP client
    http_client: HyperHttpsClient,
    /// URI
    uri: Uri,
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
            http_client: client,
            uri: url,
        }
    }
}

/// Default query format for HTTP
const HTTP_DEFAULT_FORMAT: Format = Format::TabSep;

#[async_trait]
impl Interface for Http {
    #[tracing::instrument(skip(self))]
    async fn ping(&self) -> bool {
        let req = Request::builder()
            .uri(&self.uri)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        match self.http_client.request(req).await {
            Ok(res) => res.status().is_success(),
            Err(_) => false,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, query: Query) -> Result<QueryResponse, Error> {
        let mut req_builder = hyper::Request::builder();

        if let Some(db) = &query.db {
            const HEADER_DEFAULT_DB: &str = "X-ClickHouse-Database";
            req_builder = req_builder.header(HEADER_DEFAULT_DB, db);
        }

        if let Some((username, password)) = &query.credentials {
            const HEADER_USER: &str = "X-ClickHouse-User";
            const HEADER_PASSWORD: &str = "X-ClickHouse-Key";
            req_builder = req_builder.header(HEADER_USER, username);
            req_builder = req_builder.header(HEADER_PASSWORD, password);
        }

        if let Some(format) = &query.format {
            const HEADER_FORMAT: &str = "X-ClickHouse-Format";
            req_builder = req_builder.header(HEADER_FORMAT, format.to_string());
        }

        if let Some(compression) = &query.compress_request {
            const HEADER_CONTENT_ENC: &str = "Content-Encoding";
            req_builder = req_builder.header(HEADER_CONTENT_ENC, compression.to_string());
        }

        if let Some(compression) = &query.compress_response {
            const HEADER_ACCEPT_ENC: &str = "Accept-Encoding";
            req_builder = req_builder.header(HEADER_ACCEPT_ENC, compression.to_string());
        }

        let uri = {
            let uri = &self.uri;
            let scheme = uri.scheme().ok_or(Error::new("misisng scheme"))?.clone();
            let auth = uri
                .authority()
                .ok_or(Error::new("missing authority"))?
                .clone();
            let pq = format!("/?query={}", urlencoding::encode(&query.statement));
            Uri::builder()
                .scheme(scheme)
                .authority(auth)
                .path_and_query(pq)
                .build()?
        };
        let body = if let Some(data) = query.data {
            let format = query.format.unwrap_or(HTTP_DEFAULT_FORMAT);
            let bytes: Vec<u8> = data.to_bytes(format)?;
            req_builder = req_builder.header("Content-Length", bytes.len());
            Body::from(bytes)
        } else {
            req_builder = req_builder.header("Content-Length", 0);
            Body::empty()
        };
        let req = req_builder.method("POST").uri(uri).body(body)?;

        trace!(request = ?req, "sending HTTP request");
        let res = self.http_client.request(req).await?;
        let res_status = res.status();
        let res_body = hyper::body::to_bytes(res.into_body()).await?;

        if res_status.is_success() {
            let res = QueryResponse::new(
                query.format.unwrap_or(HTTP_DEFAULT_FORMAT),
                res_body.to_vec(),
            );
            Ok(res)
        } else {
            let res_body_str = String::from_utf8(res_body.to_vec())?;
            error!(error = res_body_str, "query failed");
            Err(Error::new(res_body_str.as_str()))
        }
    }
}

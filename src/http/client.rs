//! HTP client

use hyper::{Body, Request, Uri};

/// Clickhouse client
#[derive(Debug, Clone)]
pub struct Client {
    /// URL
    pub url: Uri,
    /// Database
    pub db: Option<String>,
    /// Credentials
    pub credentials: Option<(String, String)>,
    /// HTTP client
    pub http_client: HttpClient,
}

/// Hyper client signature
type HttpClient = hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>;

impl Client {
    /// Instantiates a new [Client]
    pub fn new(url: &str) -> Self {
        let url: Uri = url.parse().unwrap();
        let https_conn = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let http_client = hyper::Client::<_, hyper::Body>::builder().build(https_conn);

        Self {
            url,
            db: None,
            credentials: None,
            http_client,
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

impl Client {
    /// Tests the connection
    ///
    /// This sends a GET rquest to `.../`
    /// TODO: should be /ping
    pub async fn ping(&self) -> bool {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_local() {
        let client = Client::new("http://localhost:8123").database("test");
        assert!(client.ping().await);
    }
}

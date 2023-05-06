//! HTTP operations
//!
//! # Examples
//!
//! GET query with param:
//! ```sh
//! curl 'http://localhost:8123/?query=SELECT%201'
//! ```
//!
//! GET query wit body
//! ```sh
//! echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
//! ```
//!
//! POST query with param:
//! ```sh
//! echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
//! echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
//! $ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
//! echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
//! ```
//!
//! # Settings
//!
//! ## HTTP compression
//!
//! Clickhouse supports HTTP compression. The request can pass the `Accept-Encoding` header. Valid values are:
//! gzip, br, deflate, xz, zstd, lz4, bz2, snappy.
//!
//! To compress the response, the CH setting `enable_http_compression`, and the header `Accept-Encoding` must be set.
//!
//! ## Database
//!
//! The database name can be passed in the URL (see below), or via a header
//!
//! ```sh
//! echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
//! ```
//!
//! ## User/password
//!
//! 3 ways to do it:
//! - HTTP basic auth: `echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-`
//! - URL params (NOT RECOMMENDED): `echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-`
//! - headers: `$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-`
//!
//! ## Other settings
//!
//! Other settings can be specified in the URL.

use hyper::Body;

use crate::error::Error;

use super::Client;

impl Client {
    // /// Inserts 1 or several [Record] inside the database
    // ///
    // /// # Example
    // ///
    // /// ```sql
    // /// INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
    // /// ```
    // pub async fn insert<T>(&self, table: &str, records: &[T]) -> Result<(), Error>
    // where
    //     T: Record,
    // {
    //     let cols = T::columns();
    //     let vals = records.iter().map(|r| r.values()).collect::<Vec<_>>();
    //     let query = format!(
    //         "INSERT INTO {} [({})] {}",
    //         table,
    //         cols.join(","),
    //         vals.iter()
    //             .map(|r| {
    //                 let r_values: Vec<_> = r.iter().map(|(_k, v)| v.to_string()).collect();
    //                 format!("({})", r_values.join(","))
    //             })
    //             .collect::<Vec<_>>()
    //             .join(",")
    //     );
    //     tracing::trace!(query, "insert query");

    //     let raw_res = self.raw_query(query).await?;

    //     // TODO: => parse row results
    //     todo!()
    // }

    // /// Selects records
    // pub async fn select<T>(&self, table: &str, where_: &str) -> Result<(), Error>
    // where
    //     T: Record,
    // {
    //     todo!();
    // }

    /// Sends a raw query
    ///
    /// # Arguments
    ///
    /// The argument is the raw query as a string
    ///
    /// # Result
    ///
    /// The result is a vector of bytes in case of success, or an error message in case of failure
    pub async fn raw_query(&self, query: impl Into<String>) -> Result<Vec<u8>, Error> {
        let mut req_builder = hyper::Request::builder().uri(&self.url).method("POST");

        // add default database
        if let Some(db) = &self.db {
            const HEADER_DEFAULT_DB: &str = "X-ClickHouse-Database";
            req_builder = req_builder.header(HEADER_DEFAULT_DB, db);
        }

        // add credentials
        if let Some((username, password)) = &self.credentials {
            const HEADER_USER: &str = "X-ClickHouse-User";
            const HEADER_PASSWORD: &str = "X-ClickHouse-Key";
            req_builder = req_builder.header(HEADER_USER, username);
            req_builder = req_builder.header(HEADER_PASSWORD, password);
        }

        let body = Body::from(query.into());
        let req = req_builder.body(body)?;

        let res = self.http_client.request(req).await?;
        let res_status = res.status();
        let body_bytes = hyper::body::to_bytes(res.into_body()).await?;

        if res_status.is_success() {
            Ok(body_bytes.to_vec())
        } else {
            let res_body_str = String::from_utf8(body_bytes.to_vec())?;
            Err(Error(res_body_str))
        }
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;
    use tokio::sync::OnceCell;

    use super::*;

    // DB structure
    //
    // Those tests require a running CLickhouse instance

    static INITIALIZED: OnceCell<Client> = OnceCell::const_new();

    struct TestRecord {
        id: u32,
        name: String,
        timestamp: OffsetDateTime,
        metric: f32,
    }

    /// Initializes the tests
    async fn init() -> Client {
        let client = INITIALIZED
            .get_or_init(|| async { Client::new("http://localhost:8123").database("test") })
            .await
            .clone();

        // create DB
        let query_create_db = "
            CREATE DATABASE IF NOT EXISTS test
            ENGINE = MergeTree()
            COMMENT 'The test database'
        ";

        client.raw_query(query_create_db).await.unwrap();

        // create the table
        let query_create_table = "
            CREATE TABLE IF NOT EXISTS records (
                id UInt32,
                name String,
                timestamp DateTime,
                metric Float32,
            )  
            ENGINE = MergeTree()
            PRIMARY KEY (id, name)
            COMMENT 'Table for tests'
        ";
        client.raw_query(query_create_table).await.unwrap();

        client
    }

    #[tokio::test]
    async fn test_query_simple() {
        let client = init().await;
        let raw_query = "SELECT 1";
        match client.raw_query(raw_query).await {
            Ok(ok) => {
                let res_body_str = String::from_utf8(ok).unwrap();
                eprintln!("{res_body_str}");
            }
            Err(err) => {
                eprintln!("{:?}", err);
            }
        }
    }
}

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

use std::collections::HashMap;

use hyper::Body;
use tracing::{debug, error};

use crate::{
    error::Error,
    schema::{DbRowExt, DbSchema, TableSchema},
};

use super::Client;

impl DbSchema {
    /// Forms the query to create the database
    pub fn build_query_create_db(&self) -> String {
        format!(
            "CREATE DATABASE IF NOT EXISTS {} ENGINE = MergeTree()",
            self.db_name
        )
    }
}

impl TableSchema {
    /// Forms the query to create a table
    ///
    /// # Example
    ///
    /// ```sql
    ///     CREATE TABLE IF NOT EXISTS records (
    ///         id UInt32,
    ///         name String,
    ///         timestamp DateTime,
    ///         metric Float32,
    ///         null_int Nullable(UInt8),
    ///     )
    ///     ENGINE = MergeTree()
    ///     PRIMARY KEY (id, name)
    ///     COMMENT 'Table for tests'
    /// ```
    pub fn build_query_create(&self) -> String {
        let fields = self
            .cols
            .iter()
            .map(|(name, col)| format!("{} {}", name, col.ty))
            .collect::<Vec<_>>()
            .join(", ");

        let keys = self
            .cols
            .iter()
            .filter_map(|(name, col)| {
                if col.is_primary {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree() PRIMARY KEY ({})",
            self.name, fields, keys
        )
    }
}

impl Client {
    /// Sends a raw query
    ///
    /// # Arguments
    ///
    /// The argument is the raw query as a string
    ///
    /// # Result
    ///
    /// The result is a vector of bytes in case of success, or an error message in case of failure
    #[tracing::instrument(skip_all, fields(query = {let s: String = query.clone().into(); s }))]
    pub async fn raw_query(&self, query: impl Into<String> + Clone) -> Result<Vec<u8>, Error> {
        let query: String = query.into();

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

        let body = Body::from(query);
        let req = req_builder.body(body)?;

        let res = self.http_client.request(req).await?;
        let res_status = res.status();
        let body_bytes = hyper::body::to_bytes(res.into_body()).await?;

        if res_status.is_success() {
            Ok(body_bytes.to_vec())
        } else {
            let res_body_str = String::from_utf8(body_bytes.to_vec())?;
            error!(error = res_body_str, "query failed");
            Err(Error(res_body_str))
        }
    }

    /// Inserts 1 or several [Record] inside the database
    ///
    /// # Note
    ///
    /// In Clickhouse, there is no RETURNING statement
    #[tracing::instrument(skip_all, fields(records.len = records.len()))]
    pub async fn insert<T>(&self, records: &[T]) -> Result<(), Error>
    where
        T: DbRowExt,
    {
        let query = build_query_insert(records);
        let _res_bytes = self.raw_query(query).await?;
        Ok(())
    }

    /// Selects 1 or several records
    pub async fn select<T>(
        &self,
        cols: &[&str],
        opts: Option<&SelectOptions>,
    ) -> Result<Vec<T>, Error>
    where
        T: DbRowExt + Default,
    {
        // // verify that the columns are valid => will fail at runtime
        // let schema = T::schema();
        // let schema_cols = schema
        //     .cols
        //     .keys()
        //     .map(|name| name.as_str())
        //     .collect::<Vec<_>>();
        // for col in cols {
        //     if !schema_cols.contains(col) {
        //         return Err(Error::new(format!("invalid SELECT column: {col}").as_str()));
        //     }
        // }

        let mut opts = opts.cloned().unwrap_or_default();
        opts.format = Some("TabSeparatedWithNames".to_string());

        let query = build_query_select::<T>(cols, Some(&opts));
        let res_bytes = self.raw_query(query).await?;
        let res_str = String::from_utf8(res_bytes)?;

        // parse the DB results
        let mut res_cols = vec![];
        let mut res_maps = vec![];
        for (i, line) in res_str.lines().enumerate() {
            if i == 0 {
                res_cols = line.split('\t').collect();
            } else {
                let mut map = HashMap::new();
                for (j, val) in line.split('\t').enumerate() {
                    let col = *res_cols.get(j).expect("shouldn't happen");
                    map.insert(col, val);
                }
                res_maps.push(map);
            }
        }
        debug!(?res_maps, "select results");

        // parse to object T
        let mut records = vec![];
        for map in res_maps {
            let record = T::from_db_values(map).map_err(|err| Error::new(err.as_str()))?;
            records.push(record);
        }

        Ok(records)
    }
}

/// Creates the INSERT query for a record
fn build_query_insert<T>(records: &[T]) -> String
where
    T: DbRowExt,
{
    let schema = T::db_schema();
    let table = schema.name;
    let cols = schema.cols;
    let vals = records
        .iter()
        .map(|record| {
            // iterate over the records
            let values = record.db_values();
            // contains each columns value as string (in the order of columns)
            let mut values_str = vec![];
            for (col, _) in cols.iter() {
                // iterate over the columns
                values_str.push(values.get(col.as_str()).unwrap().to_sql_str());
            }
            values_str
        })
        .collect::<Vec<_>>();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table,
        cols.keys()
            .map(|col_name| { col_name.as_str() })
            .collect::<Vec<_>>()
            .join(", "),
        vals.iter()
            .map(|record_vals| { format!("({})", record_vals.join(", ")) })
            .collect::<Vec<String>>()
            .join(", "),
    )
}

#[derive(Debug, Default, Clone)]
pub struct SelectOptions {
    /// WHERE
    where_filter: Option<String>,
    /// ORDER BY
    order_by: Option<String>,
    /// FORMAT
    ///
    /// cf. list of valid formats (https://clickhouse.com/docs/en/interfaces/formats)
    format: Option<String>,
}

/// Builds a SELECT query
///
/// # Example
///
/// ```sql
/// SELECT *
/// FROM helloworld.my_first_table
/// ORDER BY timestamp
/// FORMAT TabSeparated
/// ```
pub fn build_query_select<T>(cols: &[&str], opts: Option<&SelectOptions>) -> String
where
    T: DbRowExt,
{
    let schema = T::db_schema();
    let table = schema.name;
    let cols = if cols.is_empty() {
        "*".to_string()
    } else {
        cols.join(", ")
    };

    let opts = match opts {
        Some(opts) => opts.clone(),
        None => SelectOptions::default(),
    };
    let where_filter = opts
        .where_filter
        .as_ref()
        .map(|x| format!("WHERE {x}"))
        .unwrap_or_default();
    let order_by = opts
        .order_by
        .as_ref()
        .map(|x| format!("ORDER BY {x}"))
        .unwrap_or_default();
    let format = opts
        .format
        .as_ref()
        .map(|x| format!("FORMAT {x}"))
        .unwrap_or_default();

    format!("SELECT {cols} FROM {table} {where_filter} {order_by} {format}")
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;
    use tokio::sync::OnceCell;
    use tracing::{error, info};

    use super::*;
    use crate::schema::prelude::*;

    // DB structure
    //
    // Those tests require a running CLickhouse instance

    static INIT: OnceCell<Client> = OnceCell::const_new();

    #[derive(Debug, DbRow)]
    #[db(table = "test_queries")]
    struct TestRecord {
        #[db(primary)]
        id: u32,
        name: String,
        timestamp: OffsetDateTime,
        metric: f32,
        null_int: Option<u8>,
    }

    impl Default for TestRecord {
        fn default() -> Self {
            Self {
                id: Default::default(),
                name: Default::default(),
                timestamp: OffsetDateTime::UNIX_EPOCH,
                metric: Default::default(),
                null_int: Default::default(),
            }
        }
    }

    /// Initializes the tests
    #[tracing::instrument]
    async fn init() -> Client {
        crate::tests::init_test_tracer();

        let client = INIT
            .get_or_init(|| async { Client::new("http://localhost:8123").database("test") })
            .await
            .clone();

        // create DB and tables
        let db_schema = DbSchema::new("test").table(<TestRecord as DbRowExt>::db_schema());
        let query = db_schema.build_query_create_db();
        client.raw_query(query).await.unwrap();
        for (_, table_schema) in db_schema.tables {
            let query = table_schema.build_query_create();
            client.raw_query(query).await.unwrap();
        }
        client
    }

    #[tokio::test]
    #[tracing::instrument]
    async fn test_query_raw() {
        let client = init().await;
        let raw_query = "SELECT 1";
        match client.raw_query(raw_query).await {
            Ok(ok) => {
                let res_body_str = String::from_utf8(ok).unwrap();
                eprintln!("{res_body_str}");
            }
            Err(err) => {
                error!(%err, "test_query_select ERROR");
                panic!("{err}")
            }
        }
        info!("test_query_raw OK");
    }

    #[tokio::test]
    #[tracing::instrument]
    async fn test_query_insert() {
        let client: Client = init().await;

        let record_1 = TestRecord {
            id: 1,
            name: "test".to_string(),
            timestamp: OffsetDateTime::now_utc(),
            metric: 1.1,
            null_int: None,
        };

        let record_2 = TestRecord {
            id: 2,
            name: "test_2".to_string(),
            timestamp: OffsetDateTime::now_utc(),
            metric: 1.2,
            null_int: None,
        };

        match client.insert(&[record_1, record_2]).await {
            Ok(_ok) => {
                info!("test_query_insert OK");
            }
            Err(err) => {
                error!(%err, "test_query_insert ERROR");
                panic!("{err}")
            }
        }
    }

    #[tokio::test]
    #[tracing::instrument]
    async fn test_query_select() {
        let client: Client = init().await;

        match client.select::<TestRecord>(&[], None).await {
            Ok(_ok) => {
                info!("test_query_select OK");
            }
            Err(err) => {
                error!(%err, "test_query_select ERROR");
                panic!("{err}")
            }
        }
    }
}

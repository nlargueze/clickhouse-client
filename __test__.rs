#![feature(prelude_import)]
//! Clickhouse client
//!
//! This crate provides a Clickhouse client.
//!
//! It relies on `hyper` for HTTP requests, `rustls` for TLS, and indirectly the tokio runtime.
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;
pub mod error {
    //! Error
    #[error("{0}")]
    pub struct Error(pub String);
    #[automatically_derived]
    impl ::core::fmt::Debug for Error {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field1_finish(f, "Error", &&self.0)
        }
    }
    #[allow(unused_qualifications)]
    impl std::error::Error for Error {}
    #[allow(unused_qualifications)]
    impl std::fmt::Display for Error {
        #[allow(clippy::used_underscore_binding)]
        fn fmt(&self, __formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            #[allow(unused_imports)]
            use thiserror::__private::{DisplayAsDisplay, PathAsDisplay};
            #[allow(unused_variables, deprecated)]
            let Self(_0) = self;
            __formatter.write_fmt(format_args!("{0}", _0.as_display()))
        }
    }
    impl Error {
        /// Creates a new error
        pub fn new(msg: &str) -> Self {
            Self(msg.to_string())
        }
    }
    impl From<hyper::http::Error> for Error {
        fn from(value: hyper::http::Error) -> Self {
            Error::new(value.to_string().as_str())
        }
    }
    impl From<hyper::http::uri::InvalidUriParts> for Error {
        fn from(value: hyper::http::uri::InvalidUriParts) -> Self {
            Error::new(value.to_string().as_str())
        }
    }
    impl From<hyper::http::uri::InvalidUri> for Error {
        fn from(value: hyper::http::uri::InvalidUri) -> Self {
            Error::new(value.to_string().as_str())
        }
    }
    impl From<hyper::Error> for Error {
        fn from(value: hyper::Error) -> Self {
            Error::new(value.to_string().as_str())
        }
    }
    impl From<std::string::FromUtf8Error> for Error {
        fn from(value: std::string::FromUtf8Error) -> Self {
            Error::new(value.to_string().as_str())
        }
    }
}
pub mod http {
    //! HTTP interface
    //!
    //! This module provides a HTTP [Client] for the ClickHouse database.
    //!
    //! The HTTP interface is documented at: [https://clickhouse.com/docs/en/interfaces/http](https://clickhouse.com/docs/en/interfaces/http).
    //!
    //! # Overview
    //!
    //! - Port 8123 by default (8443 for HTTPS)
    //! - Healthcheck: `curl http://localhost:8123/ping`
    //! - Playground at `http://localhost:8123/play`
    //! - query can be sent with a GET + query param `?query=`, or with a POST
    mod client {
        //! HTP client
        use hyper::{Body, Request, Uri};
        /// Clickhouse client
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
        #[automatically_derived]
        impl ::core::fmt::Debug for Client {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                ::core::fmt::Formatter::debug_struct_field4_finish(
                    f,
                    "Client",
                    "url",
                    &self.url,
                    "db",
                    &self.db,
                    "credentials",
                    &self.credentials,
                    "http_client",
                    &&self.http_client,
                )
            }
        }
        #[automatically_derived]
        impl ::core::clone::Clone for Client {
            #[inline]
            fn clone(&self) -> Client {
                Client {
                    url: ::core::clone::Clone::clone(&self.url),
                    db: ::core::clone::Clone::clone(&self.db),
                    credentials: ::core::clone::Clone::clone(&self.credentials),
                    http_client: ::core::clone::Clone::clone(&self.http_client),
                }
            }
        }
        /// Hyper client signature
        type HttpClient = hyper::Client<
            hyper_rustls::HttpsConnector<hyper::client::HttpConnector>,
        >;
        impl Client {
            /// Instantiates a new [Client]
            pub fn new(url: &str) -> Self {
                let url: Uri = url.parse().unwrap();
                let https_conn = hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http1()
                    .build();
                let http_client = hyper::Client::<_, hyper::Body>::builder()
                    .build(https_conn);
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
            extern crate test;
            #[cfg(test)]
            #[rustc_test_marker = "http::client::tests::test_client_connect"]
            pub const test_client_connect: test::TestDescAndFn = test::TestDescAndFn {
                desc: test::TestDesc {
                    name: test::StaticTestName(
                        "http::client::tests::test_client_connect",
                    ),
                    ignore: false,
                    ignore_message: ::core::option::Option::None,
                    source_file: "src/http/client.rs",
                    start_line: 76usize,
                    start_col: 14usize,
                    end_line: 76usize,
                    end_col: 33usize,
                    compile_fail: false,
                    no_run: false,
                    should_panic: test::ShouldPanic::No,
                    test_type: test::TestType::UnitTest,
                },
                testfn: test::StaticTestFn(|| test::assert_test_result(
                    test_client_connect(),
                )),
            };
            fn test_client_connect() {
                let body = async {
                    let client = Client::new("http://localhost:8123").database("test");
                    if !client.ping().await {
                        ::core::panicking::panic("assertion failed: client.ping().await")
                    }
                };
                let mut body = body;
                #[allow(unused_mut)]
                let mut body = unsafe {
                    ::tokio::macros::support::Pin::new_unchecked(&mut body)
                };
                let body: ::std::pin::Pin<&mut dyn ::std::future::Future<Output = ()>> = body;
                #[allow(clippy::expect_used, clippy::diverging_sub_expression)]
                {
                    return tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(body);
                }
            }
        }
    }
    mod query {
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
        use tracing::error;
        use crate::{error::Error, schema::{DbRowExt, DbSchema, TableSchema}};
        use super::Client;
        impl DbSchema {
            /// Forms the query to create the database
            pub fn build_query_create_db(&self) -> String {
                {
                    let res = ::alloc::fmt::format(
                        format_args!(
                            "CREATE DATABASE IF NOT EXISTS {0} ENGINE = MergeTree()",
                            self.db_name
                        ),
                    );
                    res
                }
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
                    .map(|(name, col)| {
                        let res = ::alloc::fmt::format(
                            format_args!("{0} {1}", name, col.ty),
                        );
                        res
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let keys = self
                    .cols
                    .iter()
                    .filter_map(|(name, col)| {
                        if col.is_primary { Some(name.to_string()) } else { None }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                {
                    let res = ::alloc::fmt::format(
                        format_args!(
                            "CREATE TABLE IF NOT EXISTS {0} ({1}) ENGINE = MergeTree() PRIMARY KEY ({2})",
                            self.name, fields, keys
                        ),
                    );
                    res
                }
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
            pub async fn raw_query(
                &self,
                query: impl Into<String> + Clone,
            ) -> Result<Vec<u8>, Error> {
                {}
                let __tracing_attr_span = {
                    use ::tracing::__macro_support::Callsite as _;
                    static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                        static META: ::tracing::Metadata<'static> = {
                            ::tracing_core::metadata::Metadata::new(
                                "raw_query",
                                "clickhouse_client::http::query",
                                tracing::Level::INFO,
                                Some("src/http/query.rs"),
                                Some(128u32),
                                Some("clickhouse_client::http::query"),
                                ::tracing_core::field::FieldSet::new(
                                    &["query"],
                                    ::tracing_core::callsite::Identifier(&CALLSITE),
                                ),
                                ::tracing::metadata::Kind::SPAN,
                            )
                        };
                        ::tracing::callsite::DefaultCallsite::new(&META)
                    };
                    let mut interest = ::tracing::subscriber::Interest::never();
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                        && {
                            interest = CALLSITE.interest();
                            !interest.is_never()
                        }
                        && ::tracing::__macro_support::__is_enabled(
                            CALLSITE.metadata(),
                            interest,
                        )
                    {
                        let meta = CALLSITE.metadata();
                        ::tracing::Span::new(
                            meta,
                            &{
                                #[allow(unused_imports)]
                                use ::tracing::field::{debug, display, Value};
                                let mut iter = meta.fields().iter();
                                meta.fields()
                                    .value_set(
                                        &[
                                            (
                                                &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                Some(
                                                    &{
                                                        let s: String = query.clone().into();
                                                        s
                                                    } as &dyn Value,
                                                ),
                                            ),
                                        ],
                                    )
                            },
                        )
                    } else {
                        let span = ::tracing::__macro_support::__disabled_span(
                            CALLSITE.metadata(),
                        );
                        {};
                        span
                    }
                };
                let __tracing_instrument_future = async move {
                    #[allow(
                        unreachable_code,
                        clippy::diverging_sub_expression,
                        clippy::let_unit_value,
                        clippy::unreachable
                    )]
                    if false {
                        let __tracing_attr_fake_return: Result<Vec<u8>, Error> = ::core::panicking::panic_fmt(
                            format_args!(
                                "internal error: entered unreachable code: {0}",
                                format_args!("this is just for type inference, and is unreachable code")
                            ),
                        );
                        return __tracing_attr_fake_return;
                    }
                    {
                        let query: String = query.into();
                        let mut req_builder = hyper::Request::builder()
                            .uri(&self.url)
                            .method("POST");
                        if let Some(db) = &self.db {
                            const HEADER_DEFAULT_DB: &str = "X-ClickHouse-Database";
                            req_builder = req_builder.header(HEADER_DEFAULT_DB, db);
                        }
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
                            {
                                use ::tracing::__macro_support::Callsite as _;
                                static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                    static META: ::tracing::Metadata<'static> = {
                                        ::tracing_core::metadata::Metadata::new(
                                            "event src/http/query.rs:159",
                                            "clickhouse_client::http::query",
                                            ::tracing::Level::ERROR,
                                            Some("src/http/query.rs"),
                                            Some(159u32),
                                            Some("clickhouse_client::http::query"),
                                            ::tracing_core::field::FieldSet::new(
                                                &["message", "error"],
                                                ::tracing_core::callsite::Identifier(&CALLSITE),
                                            ),
                                            ::tracing::metadata::Kind::EVENT,
                                        )
                                    };
                                    ::tracing::callsite::DefaultCallsite::new(&META)
                                };
                                let enabled = ::tracing::Level::ERROR
                                    <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                    && ::tracing::Level::ERROR
                                        <= ::tracing::level_filters::LevelFilter::current()
                                    && {
                                        let interest = CALLSITE.interest();
                                        !interest.is_never()
                                            && ::tracing::__macro_support::__is_enabled(
                                                CALLSITE.metadata(),
                                                interest,
                                            )
                                    };
                                if enabled {
                                    (|value_set: ::tracing::field::ValueSet| {
                                        let meta = CALLSITE.metadata();
                                        ::tracing::Event::dispatch(meta, &value_set);
                                    })({
                                        #[allow(unused_imports)]
                                        use ::tracing::field::{debug, display, Value};
                                        let mut iter = CALLSITE.metadata().fields().iter();
                                        CALLSITE
                                            .metadata()
                                            .fields()
                                            .value_set(
                                                &[
                                                    (
                                                        &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                        Some(&format_args!("query failed") as &dyn Value),
                                                    ),
                                                    (
                                                        &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                        Some(&res_body_str as &dyn Value),
                                                    ),
                                                ],
                                            )
                                    });
                                } else {
                                }
                            };
                            Err(Error(res_body_str))
                        }
                    }
                };
                if !__tracing_attr_span.is_disabled() {
                    tracing::Instrument::instrument(
                            __tracing_instrument_future,
                            __tracing_attr_span,
                        )
                        .await
                } else {
                    __tracing_instrument_future.await
                }
            }
            /// Inserts 1 or several [Record] inside the database
            ///
            /// # Note
            ///
            /// In Clickhouse, there is no RETURNING statement
            pub async fn insert<T>(&self, records: &[T]) -> Result<(), Error>
            where
                T: DbRowExt,
            {
                {}
                let __tracing_attr_span = {
                    use ::tracing::__macro_support::Callsite as _;
                    static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                        static META: ::tracing::Metadata<'static> = {
                            ::tracing_core::metadata::Metadata::new(
                                "insert",
                                "clickhouse_client::http::query",
                                tracing::Level::INFO,
                                Some("src/http/query.rs"),
                                Some(169u32),
                                Some("clickhouse_client::http::query"),
                                ::tracing_core::field::FieldSet::new(
                                    &["records.len"],
                                    ::tracing_core::callsite::Identifier(&CALLSITE),
                                ),
                                ::tracing::metadata::Kind::SPAN,
                            )
                        };
                        ::tracing::callsite::DefaultCallsite::new(&META)
                    };
                    let mut interest = ::tracing::subscriber::Interest::never();
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                        && {
                            interest = CALLSITE.interest();
                            !interest.is_never()
                        }
                        && ::tracing::__macro_support::__is_enabled(
                            CALLSITE.metadata(),
                            interest,
                        )
                    {
                        let meta = CALLSITE.metadata();
                        ::tracing::Span::new(
                            meta,
                            &{
                                #[allow(unused_imports)]
                                use ::tracing::field::{debug, display, Value};
                                let mut iter = meta.fields().iter();
                                meta.fields()
                                    .value_set(
                                        &[
                                            (
                                                &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                Some(&records.len() as &dyn Value),
                                            ),
                                        ],
                                    )
                            },
                        )
                    } else {
                        let span = ::tracing::__macro_support::__disabled_span(
                            CALLSITE.metadata(),
                        );
                        {};
                        span
                    }
                };
                let __tracing_instrument_future = async move {
                    #[allow(
                        unreachable_code,
                        clippy::diverging_sub_expression,
                        clippy::let_unit_value,
                        clippy::unreachable
                    )]
                    if false {
                        let __tracing_attr_fake_return: Result<(), Error> = ::core::panicking::panic_fmt(
                            format_args!(
                                "internal error: entered unreachable code: {0}",
                                format_args!("this is just for type inference, and is unreachable code")
                            ),
                        );
                        return __tracing_attr_fake_return;
                    }
                    {
                        let query = build_query_insert(records);
                        let _res_bytes = self.raw_query(query).await?;
                        Ok(())
                    }
                };
                if !__tracing_attr_span.is_disabled() {
                    tracing::Instrument::instrument(
                            __tracing_instrument_future,
                            __tracing_attr_span,
                        )
                        .await
                } else {
                    __tracing_instrument_future.await
                }
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
                let mut opts = opts.cloned().unwrap_or_default();
                opts.format = Some("TabSeparatedWithNames".to_string());
                let query = build_query_select::<T>(cols, Some(&opts));
                let res_bytes = self.raw_query(query).await?;
                let res_str = String::from_utf8(res_bytes)?;
                let mut res_cols = ::alloc::vec::Vec::new();
                let mut res_maps = ::alloc::vec::Vec::new();
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
                let mut records = ::alloc::vec::Vec::new();
                for map in res_maps {
                    let record = T::from_db_values(map)
                        .map_err(|err| Error::new(err.as_str()))?;
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
                    let values = record.db_values();
                    let mut values_str = ::alloc::vec::Vec::new();
                    for (col, _) in cols.iter() {
                        values_str.push(values.get(col.as_str()).unwrap().to_sql_str());
                    }
                    values_str
                })
                .collect::<Vec<_>>();
            {
                let res = ::alloc::fmt::format(
                    format_args!(
                        "INSERT INTO {0} ({1}) VALUES {2}", table, cols.keys().map(|
                        col_name | { col_name.as_str() }).collect::< Vec < _ >> ()
                        .join(", "), vals.iter().map(| record_vals | { { let res =
                        ::alloc::fmt::format(format_args!("({0})", record_vals
                        .join(", "))); res } }).collect::< Vec < String >> ().join(", ")
                    ),
                );
                res
            }
        }
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
        #[automatically_derived]
        impl ::core::fmt::Debug for SelectOptions {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                ::core::fmt::Formatter::debug_struct_field3_finish(
                    f,
                    "SelectOptions",
                    "where_filter",
                    &self.where_filter,
                    "order_by",
                    &self.order_by,
                    "format",
                    &&self.format,
                )
            }
        }
        #[automatically_derived]
        impl ::core::default::Default for SelectOptions {
            #[inline]
            fn default() -> SelectOptions {
                SelectOptions {
                    where_filter: ::core::default::Default::default(),
                    order_by: ::core::default::Default::default(),
                    format: ::core::default::Default::default(),
                }
            }
        }
        #[automatically_derived]
        impl ::core::clone::Clone for SelectOptions {
            #[inline]
            fn clone(&self) -> SelectOptions {
                SelectOptions {
                    where_filter: ::core::clone::Clone::clone(&self.where_filter),
                    order_by: ::core::clone::Clone::clone(&self.order_by),
                    format: ::core::clone::Clone::clone(&self.format),
                }
            }
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
        pub fn build_query_select<T>(
            cols: &[&str],
            opts: Option<&SelectOptions>,
        ) -> String
        where
            T: DbRowExt,
        {
            let schema = T::db_schema();
            let table = schema.name;
            let cols = if cols.is_empty() { "*".to_string() } else { cols.join(", ") };
            let opts = match opts {
                Some(opts) => opts.clone(),
                None => SelectOptions::default(),
            };
            let where_filter = opts
                .where_filter
                .as_ref()
                .map(|x| {
                    let res = ::alloc::fmt::format(format_args!("WHERE {0}", x));
                    res
                })
                .unwrap_or_default();
            let order_by = opts
                .order_by
                .as_ref()
                .map(|x| {
                    let res = ::alloc::fmt::format(format_args!("ORDER BY {0}", x));
                    res
                })
                .unwrap_or_default();
            let format = opts
                .format
                .as_ref()
                .map(|x| {
                    let res = ::alloc::fmt::format(format_args!("FORMAT {0}", x));
                    res
                })
                .unwrap_or_default();
            {
                let res = ::alloc::fmt::format(
                    format_args!(
                        "SELECT {0} FROM {1} {2} {3} {4}", cols, table, where_filter,
                        order_by, format
                    ),
                );
                res
            }
        }
        #[cfg(test)]
        mod tests {
            use time::OffsetDateTime;
            use tokio::sync::OnceCell;
            use tracing::{error, info};
            use super::*;
            use crate::schema::prelude::*;
            static INIT: OnceCell<Client> = OnceCell::const_new();
            #[db(table = "test_queries")]
            struct TestRecord {
                #[db(primary)]
                id: u32,
                name: String,
                timestamp: OffsetDateTime,
                metric: f32,
                null_int: Option<u8>,
            }
            #[automatically_derived]
            impl ::core::fmt::Debug for TestRecord {
                fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                    ::core::fmt::Formatter::debug_struct_field5_finish(
                        f,
                        "TestRecord",
                        "id",
                        &self.id,
                        "name",
                        &self.name,
                        "timestamp",
                        &self.timestamp,
                        "metric",
                        &self.metric,
                        "null_int",
                        &&self.null_int,
                    )
                }
            }
            impl DbRowExt for TestRecord {
                fn db_schema() -> TableSchema {
                    TableSchema::new("test_queries")
                        .column(ColSchema {
                            name: "id".to_string(),
                            ty: "UInt32".to_string(),
                            is_primary: true,
                        })
                        .column(ColSchema {
                            name: "name".to_string(),
                            ty: "String".to_string(),
                            is_primary: false,
                        })
                        .column(ColSchema {
                            name: "timestamp".to_string(),
                            ty: "DateTime".to_string(),
                            is_primary: false,
                        })
                        .column(ColSchema {
                            name: "metric".to_string(),
                            ty: "Float32".to_string(),
                            is_primary: false,
                        })
                        .column(ColSchema {
                            name: "null_int".to_string(),
                            ty: "Nullable(UInt8)".to_string(),
                            is_primary: false,
                        })
                }
                fn db_values(
                    &self,
                ) -> ::std::collections::HashMap<&'static str, Box<&'_ dyn DbType>> {
                    let mut map: ::std::collections::HashMap<
                        &str,
                        Box<&'_ dyn DbType>,
                    > = ::std::collections::HashMap::new();
                    map.insert("id", Box::new(&self.id));
                    map.insert("name", Box::new(&self.name));
                    map.insert("timestamp", Box::new(&self.timestamp));
                    map.insert("metric", Box::new(&self.metric));
                    map.insert("null_int", Box::new(&self.null_int));
                    map
                }
                fn from_db_values(
                    values: ::std::collections::HashMap<&str, &str>,
                ) -> ::std::result::Result<Self, String>
                where
                    Self: Sized + Default,
                {
                    let mut record = Self::default();
                    record.id = <u32 as DbType>::from_sql_str(values["id"])?;
                    record.name = <String as DbType>::from_sql_str(values["name"])?;
                    record
                        .timestamp = <OffsetDateTime as DbType>::from_sql_str(
                        values["timestamp"],
                    )?;
                    record.metric = <f32 as DbType>::from_sql_str(values["metric"])?;
                    record
                        .null_int = <Option<
                        u8,
                    > as DbType>::from_sql_str(values["null_int"])?;
                    Ok(record)
                }
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
            async fn init() -> Client {
                {}
                let __tracing_attr_span = {
                    use ::tracing::__macro_support::Callsite as _;
                    static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                        static META: ::tracing::Metadata<'static> = {
                            ::tracing_core::metadata::Metadata::new(
                                "init",
                                "clickhouse_client::http::query::tests",
                                tracing::Level::INFO,
                                Some("src/http/query.rs"),
                                Some(368u32),
                                Some("clickhouse_client::http::query::tests"),
                                ::tracing_core::field::FieldSet::new(
                                    &[],
                                    ::tracing_core::callsite::Identifier(&CALLSITE),
                                ),
                                ::tracing::metadata::Kind::SPAN,
                            )
                        };
                        ::tracing::callsite::DefaultCallsite::new(&META)
                    };
                    let mut interest = ::tracing::subscriber::Interest::never();
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                        && {
                            interest = CALLSITE.interest();
                            !interest.is_never()
                        }
                        && ::tracing::__macro_support::__is_enabled(
                            CALLSITE.metadata(),
                            interest,
                        )
                    {
                        let meta = CALLSITE.metadata();
                        ::tracing::Span::new(meta, &{ meta.fields().value_set(&[]) })
                    } else {
                        let span = ::tracing::__macro_support::__disabled_span(
                            CALLSITE.metadata(),
                        );
                        {};
                        span
                    }
                };
                let __tracing_instrument_future = async move {
                    #[allow(
                        unreachable_code,
                        clippy::diverging_sub_expression,
                        clippy::let_unit_value,
                        clippy::unreachable
                    )]
                    if false {
                        let __tracing_attr_fake_return: Client = ::core::panicking::panic_fmt(
                            format_args!(
                                "internal error: entered unreachable code: {0}",
                                format_args!("this is just for type inference, and is unreachable code")
                            ),
                        );
                        return __tracing_attr_fake_return;
                    }
                    {
                        crate::tests::init_test_tracer();
                        let client = INIT
                            .get_or_init(|| async {
                                Client::new("http://localhost:8123").database("test")
                            })
                            .await
                            .clone();
                        let db_schema = DbSchema::new("test")
                            .table(<TestRecord as DbRowExt>::db_schema());
                        let query = db_schema.build_query_create_db();
                        client.raw_query(query).await.unwrap();
                        for (_, table_schema) in db_schema.tables {
                            let query = table_schema.build_query_create();
                            client.raw_query(query).await.unwrap();
                        }
                        client
                    }
                };
                if !__tracing_attr_span.is_disabled() {
                    tracing::Instrument::instrument(
                            __tracing_instrument_future,
                            __tracing_attr_span,
                        )
                        .await
                } else {
                    __tracing_instrument_future.await
                }
            }
            extern crate test;
            #[cfg(test)]
            #[rustc_test_marker = "http::query::tests::test_query_raw"]
            pub const test_query_raw: test::TestDescAndFn = test::TestDescAndFn {
                desc: test::TestDesc {
                    name: test::StaticTestName("http::query::tests::test_query_raw"),
                    ignore: false,
                    ignore_message: ::core::option::Option::None,
                    source_file: "src/http/query.rs",
                    start_line: 390usize,
                    start_col: 14usize,
                    end_line: 390usize,
                    end_col: 28usize,
                    compile_fail: false,
                    no_run: false,
                    should_panic: test::ShouldPanic::No,
                    test_type: test::TestType::UnitTest,
                },
                testfn: test::StaticTestFn(|| test::assert_test_result(test_query_raw())),
            };
            fn test_query_raw() {
                {}
                #[allow(clippy::suspicious_else_formatting)]
                {
                    let __tracing_attr_span;
                    let __tracing_attr_guard;
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                    {
                        __tracing_attr_span = {
                            use ::tracing::__macro_support::Callsite as _;
                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                static META: ::tracing::Metadata<'static> = {
                                    ::tracing_core::metadata::Metadata::new(
                                        "test_query_raw",
                                        "clickhouse_client::http::query::tests",
                                        tracing::Level::INFO,
                                        Some("src/http/query.rs"),
                                        Some(389u32),
                                        Some("clickhouse_client::http::query::tests"),
                                        ::tracing_core::field::FieldSet::new(
                                            &[],
                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                        ),
                                        ::tracing::metadata::Kind::SPAN,
                                    )
                                };
                                ::tracing::callsite::DefaultCallsite::new(&META)
                            };
                            let mut interest = ::tracing::subscriber::Interest::never();
                            if tracing::Level::INFO
                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                && tracing::Level::INFO
                                    <= ::tracing::level_filters::LevelFilter::current()
                                && {
                                    interest = CALLSITE.interest();
                                    !interest.is_never()
                                }
                                && ::tracing::__macro_support::__is_enabled(
                                    CALLSITE.metadata(),
                                    interest,
                                )
                            {
                                let meta = CALLSITE.metadata();
                                ::tracing::Span::new(
                                    meta,
                                    &{ meta.fields().value_set(&[]) },
                                )
                            } else {
                                let span = ::tracing::__macro_support::__disabled_span(
                                    CALLSITE.metadata(),
                                );
                                {};
                                span
                            }
                        };
                        __tracing_attr_guard = __tracing_attr_span.enter();
                    }
                    #[warn(clippy::suspicious_else_formatting)]
                    {
                        #[allow(
                            unreachable_code,
                            clippy::diverging_sub_expression,
                            clippy::let_unit_value,
                            clippy::unreachable
                        )]
                        if false {
                            let __tracing_attr_fake_return: () = ::core::panicking::panic_fmt(
                                format_args!(
                                    "internal error: entered unreachable code: {0}",
                                    format_args!("this is just for type inference, and is unreachable code")
                                ),
                            );
                            return __tracing_attr_fake_return;
                        }
                        {
                            let body = async {
                                let client = init().await;
                                let raw_query = "SELECT 1";
                                match client.raw_query(raw_query).await {
                                    Ok(ok) => {
                                        let res_body_str = String::from_utf8(ok).unwrap();
                                        {
                                            ::std::io::_eprint(format_args!("{0}\n", res_body_str));
                                        };
                                    }
                                    Err(err) => {
                                        {
                                            use ::tracing::__macro_support::Callsite as _;
                                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                                static META: ::tracing::Metadata<'static> = {
                                                    ::tracing_core::metadata::Metadata::new(
                                                        "event src/http/query.rs:399",
                                                        "clickhouse_client::http::query::tests",
                                                        ::tracing::Level::ERROR,
                                                        Some("src/http/query.rs"),
                                                        Some(399u32),
                                                        Some("clickhouse_client::http::query::tests"),
                                                        ::tracing_core::field::FieldSet::new(
                                                            &["message", "err"],
                                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                                        ),
                                                        ::tracing::metadata::Kind::EVENT,
                                                    )
                                                };
                                                ::tracing::callsite::DefaultCallsite::new(&META)
                                            };
                                            let enabled = ::tracing::Level::ERROR
                                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                                && ::tracing::Level::ERROR
                                                    <= ::tracing::level_filters::LevelFilter::current()
                                                && {
                                                    let interest = CALLSITE.interest();
                                                    !interest.is_never()
                                                        && ::tracing::__macro_support::__is_enabled(
                                                            CALLSITE.metadata(),
                                                            interest,
                                                        )
                                                };
                                            if enabled {
                                                (|value_set: ::tracing::field::ValueSet| {
                                                    let meta = CALLSITE.metadata();
                                                    ::tracing::Event::dispatch(meta, &value_set);
                                                })({
                                                    #[allow(unused_imports)]
                                                    use ::tracing::field::{debug, display, Value};
                                                    let mut iter = CALLSITE.metadata().fields().iter();
                                                    CALLSITE
                                                        .metadata()
                                                        .fields()
                                                        .value_set(
                                                            &[
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&format_args!("test_query_select ERROR") as &dyn Value),
                                                                ),
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&display(&err) as &dyn Value),
                                                                ),
                                                            ],
                                                        )
                                                });
                                            } else {
                                            }
                                        };
                                        ::core::panicking::panic_fmt(format_args!("{0}", err))
                                    }
                                }
                                {
                                    use ::tracing::__macro_support::Callsite as _;
                                    static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                        static META: ::tracing::Metadata<'static> = {
                                            ::tracing_core::metadata::Metadata::new(
                                                "event src/http/query.rs:403",
                                                "clickhouse_client::http::query::tests",
                                                ::tracing::Level::INFO,
                                                Some("src/http/query.rs"),
                                                Some(403u32),
                                                Some("clickhouse_client::http::query::tests"),
                                                ::tracing_core::field::FieldSet::new(
                                                    &["message"],
                                                    ::tracing_core::callsite::Identifier(&CALLSITE),
                                                ),
                                                ::tracing::metadata::Kind::EVENT,
                                            )
                                        };
                                        ::tracing::callsite::DefaultCallsite::new(&META)
                                    };
                                    let enabled = ::tracing::Level::INFO
                                        <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                        && ::tracing::Level::INFO
                                            <= ::tracing::level_filters::LevelFilter::current()
                                        && {
                                            let interest = CALLSITE.interest();
                                            !interest.is_never()
                                                && ::tracing::__macro_support::__is_enabled(
                                                    CALLSITE.metadata(),
                                                    interest,
                                                )
                                        };
                                    if enabled {
                                        (|value_set: ::tracing::field::ValueSet| {
                                            let meta = CALLSITE.metadata();
                                            ::tracing::Event::dispatch(meta, &value_set);
                                        })({
                                            #[allow(unused_imports)]
                                            use ::tracing::field::{debug, display, Value};
                                            let mut iter = CALLSITE.metadata().fields().iter();
                                            CALLSITE
                                                .metadata()
                                                .fields()
                                                .value_set(
                                                    &[
                                                        (
                                                            &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                            Some(&format_args!("test_query_raw OK") as &dyn Value),
                                                        ),
                                                    ],
                                                )
                                        });
                                    } else {
                                    }
                                };
                            };
                            let mut body = body;
                            #[allow(unused_mut)]
                            let mut body = unsafe {
                                ::tokio::macros::support::Pin::new_unchecked(&mut body)
                            };
                            let body: ::std::pin::Pin<
                                &mut dyn ::std::future::Future<Output = ()>,
                            > = body;
                            #[allow(
                                clippy::expect_used,
                                clippy::diverging_sub_expression
                            )]
                            {
                                return tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("Failed building the Runtime")
                                    .block_on(body);
                            }
                        }
                    }
                }
            }
            extern crate test;
            #[cfg(test)]
            #[rustc_test_marker = "http::query::tests::test_query_insert"]
            pub const test_query_insert: test::TestDescAndFn = test::TestDescAndFn {
                desc: test::TestDesc {
                    name: test::StaticTestName("http::query::tests::test_query_insert"),
                    ignore: false,
                    ignore_message: ::core::option::Option::None,
                    source_file: "src/http/query.rs",
                    start_line: 408usize,
                    start_col: 14usize,
                    end_line: 408usize,
                    end_col: 31usize,
                    compile_fail: false,
                    no_run: false,
                    should_panic: test::ShouldPanic::No,
                    test_type: test::TestType::UnitTest,
                },
                testfn: test::StaticTestFn(|| test::assert_test_result(
                    test_query_insert(),
                )),
            };
            fn test_query_insert() {
                {}
                #[allow(clippy::suspicious_else_formatting)]
                {
                    let __tracing_attr_span;
                    let __tracing_attr_guard;
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                    {
                        __tracing_attr_span = {
                            use ::tracing::__macro_support::Callsite as _;
                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                static META: ::tracing::Metadata<'static> = {
                                    ::tracing_core::metadata::Metadata::new(
                                        "test_query_insert",
                                        "clickhouse_client::http::query::tests",
                                        tracing::Level::INFO,
                                        Some("src/http/query.rs"),
                                        Some(407u32),
                                        Some("clickhouse_client::http::query::tests"),
                                        ::tracing_core::field::FieldSet::new(
                                            &[],
                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                        ),
                                        ::tracing::metadata::Kind::SPAN,
                                    )
                                };
                                ::tracing::callsite::DefaultCallsite::new(&META)
                            };
                            let mut interest = ::tracing::subscriber::Interest::never();
                            if tracing::Level::INFO
                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                && tracing::Level::INFO
                                    <= ::tracing::level_filters::LevelFilter::current()
                                && {
                                    interest = CALLSITE.interest();
                                    !interest.is_never()
                                }
                                && ::tracing::__macro_support::__is_enabled(
                                    CALLSITE.metadata(),
                                    interest,
                                )
                            {
                                let meta = CALLSITE.metadata();
                                ::tracing::Span::new(
                                    meta,
                                    &{ meta.fields().value_set(&[]) },
                                )
                            } else {
                                let span = ::tracing::__macro_support::__disabled_span(
                                    CALLSITE.metadata(),
                                );
                                {};
                                span
                            }
                        };
                        __tracing_attr_guard = __tracing_attr_span.enter();
                    }
                    #[warn(clippy::suspicious_else_formatting)]
                    {
                        #[allow(
                            unreachable_code,
                            clippy::diverging_sub_expression,
                            clippy::let_unit_value,
                            clippy::unreachable
                        )]
                        if false {
                            let __tracing_attr_fake_return: () = ::core::panicking::panic_fmt(
                                format_args!(
                                    "internal error: entered unreachable code: {0}",
                                    format_args!("this is just for type inference, and is unreachable code")
                                ),
                            );
                            return __tracing_attr_fake_return;
                        }
                        {
                            let body = async {
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
                                        {
                                            use ::tracing::__macro_support::Callsite as _;
                                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                                static META: ::tracing::Metadata<'static> = {
                                                    ::tracing_core::metadata::Metadata::new(
                                                        "event src/http/query.rs:429",
                                                        "clickhouse_client::http::query::tests",
                                                        ::tracing::Level::INFO,
                                                        Some("src/http/query.rs"),
                                                        Some(429u32),
                                                        Some("clickhouse_client::http::query::tests"),
                                                        ::tracing_core::field::FieldSet::new(
                                                            &["message"],
                                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                                        ),
                                                        ::tracing::metadata::Kind::EVENT,
                                                    )
                                                };
                                                ::tracing::callsite::DefaultCallsite::new(&META)
                                            };
                                            let enabled = ::tracing::Level::INFO
                                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                                && ::tracing::Level::INFO
                                                    <= ::tracing::level_filters::LevelFilter::current()
                                                && {
                                                    let interest = CALLSITE.interest();
                                                    !interest.is_never()
                                                        && ::tracing::__macro_support::__is_enabled(
                                                            CALLSITE.metadata(),
                                                            interest,
                                                        )
                                                };
                                            if enabled {
                                                (|value_set: ::tracing::field::ValueSet| {
                                                    let meta = CALLSITE.metadata();
                                                    ::tracing::Event::dispatch(meta, &value_set);
                                                })({
                                                    #[allow(unused_imports)]
                                                    use ::tracing::field::{debug, display, Value};
                                                    let mut iter = CALLSITE.metadata().fields().iter();
                                                    CALLSITE
                                                        .metadata()
                                                        .fields()
                                                        .value_set(
                                                            &[
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&format_args!("test_query_insert OK") as &dyn Value),
                                                                ),
                                                            ],
                                                        )
                                                });
                                            } else {
                                            }
                                        };
                                    }
                                    Err(err) => {
                                        {
                                            use ::tracing::__macro_support::Callsite as _;
                                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                                static META: ::tracing::Metadata<'static> = {
                                                    ::tracing_core::metadata::Metadata::new(
                                                        "event src/http/query.rs:432",
                                                        "clickhouse_client::http::query::tests",
                                                        ::tracing::Level::ERROR,
                                                        Some("src/http/query.rs"),
                                                        Some(432u32),
                                                        Some("clickhouse_client::http::query::tests"),
                                                        ::tracing_core::field::FieldSet::new(
                                                            &["message", "err"],
                                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                                        ),
                                                        ::tracing::metadata::Kind::EVENT,
                                                    )
                                                };
                                                ::tracing::callsite::DefaultCallsite::new(&META)
                                            };
                                            let enabled = ::tracing::Level::ERROR
                                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                                && ::tracing::Level::ERROR
                                                    <= ::tracing::level_filters::LevelFilter::current()
                                                && {
                                                    let interest = CALLSITE.interest();
                                                    !interest.is_never()
                                                        && ::tracing::__macro_support::__is_enabled(
                                                            CALLSITE.metadata(),
                                                            interest,
                                                        )
                                                };
                                            if enabled {
                                                (|value_set: ::tracing::field::ValueSet| {
                                                    let meta = CALLSITE.metadata();
                                                    ::tracing::Event::dispatch(meta, &value_set);
                                                })({
                                                    #[allow(unused_imports)]
                                                    use ::tracing::field::{debug, display, Value};
                                                    let mut iter = CALLSITE.metadata().fields().iter();
                                                    CALLSITE
                                                        .metadata()
                                                        .fields()
                                                        .value_set(
                                                            &[
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&format_args!("test_query_insert ERROR") as &dyn Value),
                                                                ),
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&display(&err) as &dyn Value),
                                                                ),
                                                            ],
                                                        )
                                                });
                                            } else {
                                            }
                                        };
                                        ::core::panicking::panic_fmt(format_args!("{0}", err))
                                    }
                                }
                            };
                            let mut body = body;
                            #[allow(unused_mut)]
                            let mut body = unsafe {
                                ::tokio::macros::support::Pin::new_unchecked(&mut body)
                            };
                            let body: ::std::pin::Pin<
                                &mut dyn ::std::future::Future<Output = ()>,
                            > = body;
                            #[allow(
                                clippy::expect_used,
                                clippy::diverging_sub_expression
                            )]
                            {
                                return tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("Failed building the Runtime")
                                    .block_on(body);
                            }
                        }
                    }
                }
            }
            extern crate test;
            #[cfg(test)]
            #[rustc_test_marker = "http::query::tests::test_query_select"]
            pub const test_query_select: test::TestDescAndFn = test::TestDescAndFn {
                desc: test::TestDesc {
                    name: test::StaticTestName("http::query::tests::test_query_select"),
                    ignore: false,
                    ignore_message: ::core::option::Option::None,
                    source_file: "src/http/query.rs",
                    start_line: 440usize,
                    start_col: 14usize,
                    end_line: 440usize,
                    end_col: 31usize,
                    compile_fail: false,
                    no_run: false,
                    should_panic: test::ShouldPanic::No,
                    test_type: test::TestType::UnitTest,
                },
                testfn: test::StaticTestFn(|| test::assert_test_result(
                    test_query_select(),
                )),
            };
            fn test_query_select() {
                {}
                #[allow(clippy::suspicious_else_formatting)]
                {
                    let __tracing_attr_span;
                    let __tracing_attr_guard;
                    if tracing::Level::INFO <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                    {
                        __tracing_attr_span = {
                            use ::tracing::__macro_support::Callsite as _;
                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                static META: ::tracing::Metadata<'static> = {
                                    ::tracing_core::metadata::Metadata::new(
                                        "test_query_select",
                                        "clickhouse_client::http::query::tests",
                                        tracing::Level::INFO,
                                        Some("src/http/query.rs"),
                                        Some(439u32),
                                        Some("clickhouse_client::http::query::tests"),
                                        ::tracing_core::field::FieldSet::new(
                                            &[],
                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                        ),
                                        ::tracing::metadata::Kind::SPAN,
                                    )
                                };
                                ::tracing::callsite::DefaultCallsite::new(&META)
                            };
                            let mut interest = ::tracing::subscriber::Interest::never();
                            if tracing::Level::INFO
                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                && tracing::Level::INFO
                                    <= ::tracing::level_filters::LevelFilter::current()
                                && {
                                    interest = CALLSITE.interest();
                                    !interest.is_never()
                                }
                                && ::tracing::__macro_support::__is_enabled(
                                    CALLSITE.metadata(),
                                    interest,
                                )
                            {
                                let meta = CALLSITE.metadata();
                                ::tracing::Span::new(
                                    meta,
                                    &{ meta.fields().value_set(&[]) },
                                )
                            } else {
                                let span = ::tracing::__macro_support::__disabled_span(
                                    CALLSITE.metadata(),
                                );
                                {};
                                span
                            }
                        };
                        __tracing_attr_guard = __tracing_attr_span.enter();
                    }
                    #[warn(clippy::suspicious_else_formatting)]
                    {
                        #[allow(
                            unreachable_code,
                            clippy::diverging_sub_expression,
                            clippy::let_unit_value,
                            clippy::unreachable
                        )]
                        if false {
                            let __tracing_attr_fake_return: () = ::core::panicking::panic_fmt(
                                format_args!(
                                    "internal error: entered unreachable code: {0}",
                                    format_args!("this is just for type inference, and is unreachable code")
                                ),
                            );
                            return __tracing_attr_fake_return;
                        }
                        {
                            let body = async {
                                let client: Client = init().await;
                                match client.select::<TestRecord>(&[], None).await {
                                    Ok(_ok) => {
                                        {
                                            use ::tracing::__macro_support::Callsite as _;
                                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                                static META: ::tracing::Metadata<'static> = {
                                                    ::tracing_core::metadata::Metadata::new(
                                                        "event src/http/query.rs:445",
                                                        "clickhouse_client::http::query::tests",
                                                        ::tracing::Level::INFO,
                                                        Some("src/http/query.rs"),
                                                        Some(445u32),
                                                        Some("clickhouse_client::http::query::tests"),
                                                        ::tracing_core::field::FieldSet::new(
                                                            &["message"],
                                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                                        ),
                                                        ::tracing::metadata::Kind::EVENT,
                                                    )
                                                };
                                                ::tracing::callsite::DefaultCallsite::new(&META)
                                            };
                                            let enabled = ::tracing::Level::INFO
                                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                                && ::tracing::Level::INFO
                                                    <= ::tracing::level_filters::LevelFilter::current()
                                                && {
                                                    let interest = CALLSITE.interest();
                                                    !interest.is_never()
                                                        && ::tracing::__macro_support::__is_enabled(
                                                            CALLSITE.metadata(),
                                                            interest,
                                                        )
                                                };
                                            if enabled {
                                                (|value_set: ::tracing::field::ValueSet| {
                                                    let meta = CALLSITE.metadata();
                                                    ::tracing::Event::dispatch(meta, &value_set);
                                                })({
                                                    #[allow(unused_imports)]
                                                    use ::tracing::field::{debug, display, Value};
                                                    let mut iter = CALLSITE.metadata().fields().iter();
                                                    CALLSITE
                                                        .metadata()
                                                        .fields()
                                                        .value_set(
                                                            &[
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&format_args!("test_query_select OK") as &dyn Value),
                                                                ),
                                                            ],
                                                        )
                                                });
                                            } else {
                                            }
                                        };
                                    }
                                    Err(err) => {
                                        {
                                            use ::tracing::__macro_support::Callsite as _;
                                            static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                                                static META: ::tracing::Metadata<'static> = {
                                                    ::tracing_core::metadata::Metadata::new(
                                                        "event src/http/query.rs:448",
                                                        "clickhouse_client::http::query::tests",
                                                        ::tracing::Level::ERROR,
                                                        Some("src/http/query.rs"),
                                                        Some(448u32),
                                                        Some("clickhouse_client::http::query::tests"),
                                                        ::tracing_core::field::FieldSet::new(
                                                            &["message", "err"],
                                                            ::tracing_core::callsite::Identifier(&CALLSITE),
                                                        ),
                                                        ::tracing::metadata::Kind::EVENT,
                                                    )
                                                };
                                                ::tracing::callsite::DefaultCallsite::new(&META)
                                            };
                                            let enabled = ::tracing::Level::ERROR
                                                <= ::tracing::level_filters::STATIC_MAX_LEVEL
                                                && ::tracing::Level::ERROR
                                                    <= ::tracing::level_filters::LevelFilter::current()
                                                && {
                                                    let interest = CALLSITE.interest();
                                                    !interest.is_never()
                                                        && ::tracing::__macro_support::__is_enabled(
                                                            CALLSITE.metadata(),
                                                            interest,
                                                        )
                                                };
                                            if enabled {
                                                (|value_set: ::tracing::field::ValueSet| {
                                                    let meta = CALLSITE.metadata();
                                                    ::tracing::Event::dispatch(meta, &value_set);
                                                })({
                                                    #[allow(unused_imports)]
                                                    use ::tracing::field::{debug, display, Value};
                                                    let mut iter = CALLSITE.metadata().fields().iter();
                                                    CALLSITE
                                                        .metadata()
                                                        .fields()
                                                        .value_set(
                                                            &[
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&format_args!("test_query_select ERROR") as &dyn Value),
                                                                ),
                                                                (
                                                                    &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                                                    Some(&display(&err) as &dyn Value),
                                                                ),
                                                            ],
                                                        )
                                                });
                                            } else {
                                            }
                                        };
                                        ::core::panicking::panic_fmt(format_args!("{0}", err))
                                    }
                                }
                            };
                            let mut body = body;
                            #[allow(unused_mut)]
                            let mut body = unsafe {
                                ::tokio::macros::support::Pin::new_unchecked(&mut body)
                            };
                            let body: ::std::pin::Pin<
                                &mut dyn ::std::future::Future<Output = ()>,
                            > = body;
                            #[allow(
                                clippy::expect_used,
                                clippy::diverging_sub_expression
                            )]
                            {
                                return tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("Failed building the Runtime")
                                    .block_on(body);
                            }
                        }
                    }
                }
            }
        }
    }
    pub use client::*;
    pub use query::*;
}
pub mod schema {
    //! DB schema
    use std::collections::HashMap;
    mod derive {
        //! Derive macro
        //!
        //! This module provides a [DbRowExt] trait which must be implemented by structs representing
        //! database records, and a [DbRow] macro to derive that trait.
        use std::collections::HashMap;
        pub use clickhouse_client_macros::DbRow;
        use super::{DbType, TableSchema};
        /// Extension trait to represent any struct as a database row
        pub trait DbRowExt {
            /// Returns the type DB schema
            fn db_schema() -> TableSchema;
            /// Returns the DB values
            fn db_values(&self) -> HashMap<&'static str, Box<&'_ dyn DbType>>;
            /// Composes the object from a map (column, value)
            fn from_db_values(values: HashMap<&str, &str>) -> Result<Self, String>
            where
                Self: Sized + Default;
        }
        #[cfg(test)]
        mod tests {
            use crate::schema::prelude::*;
            use time::OffsetDateTime;
            use tracing::info;
            use super::*;
            fn init() {
                crate::tests::init_test_tracer();
            }
            /// A sample struct that represents a DB record
            #[db(table = "test_derive")]
            struct TestRow {
                /// ID
                #[db(primary)]
                id: u8,
                /// Name
                #[db(primary, name = "name2")]
                name: String,
                /// Timestamp
                dt: OffsetDateTime,
            }
            #[automatically_derived]
            impl ::core::fmt::Debug for TestRow {
                fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                    ::core::fmt::Formatter::debug_struct_field3_finish(
                        f,
                        "TestRow",
                        "id",
                        &self.id,
                        "name",
                        &self.name,
                        "dt",
                        &&self.dt,
                    )
                }
            }
            impl DbRowExt for TestRow {
                fn db_schema() -> TableSchema {
                    TableSchema::new("test_derive")
                        .column(ColSchema {
                            name: "id".to_string(),
                            ty: "UInt8".to_string(),
                            is_primary: true,
                        })
                        .column(ColSchema {
                            name: "name2".to_string(),
                            ty: "String".to_string(),
                            is_primary: true,
                        })
                        .column(ColSchema {
                            name: "dt".to_string(),
                            ty: "DateTime".to_string(),
                            is_primary: false,
                        })
                }
                fn db_values(
                    &self,
                ) -> ::std::collections::HashMap<&'static str, Box<&'_ dyn DbType>> {
                    let mut map: ::std::collections::HashMap<
                        &str,
                        Box<&'_ dyn DbType>,
                    > = ::std::collections::HashMap::new();
                    map.insert("id", Box::new(&self.id));
                    map.insert("name2", Box::new(&self.name));
                    map.insert("dt", Box::new(&self.dt));
                    map
                }
                fn from_db_values(
                    values: ::std::collections::HashMap<&str, &str>,
                ) -> ::std::result::Result<Self, String>
                where
                    Self: Sized + Default,
                {
                    let mut record = Self::default();
                    record.id = <u8 as DbType>::from_sql_str(values["id"])?;
                    record.name = <String as DbType>::from_sql_str(values["name2"])?;
                    record.dt = <OffsetDateTime as DbType>::from_sql_str(values["dt"])?;
                    Ok(record)
                }
            }
            impl Default for TestRow {
                fn default() -> Self {
                    Self {
                        id: Default::default(),
                        name: Default::default(),
                        dt: OffsetDateTime::UNIX_EPOCH,
                    }
                }
            }
            extern crate test;
            #[cfg(test)]
            #[rustc_test_marker = "schema::derive::tests::test_derive_simple"]
            pub const test_derive_simple: test::TestDescAndFn = test::TestDescAndFn {
                desc: test::TestDesc {
                    name: test::StaticTestName(
                        "schema::derive::tests::test_derive_simple",
                    ),
                    ignore: false,
                    ignore_message: ::core::option::Option::None,
                    source_file: "src/schema/derive.rs",
                    start_line: 63usize,
                    start_col: 8usize,
                    end_line: 63usize,
                    end_col: 26usize,
                    compile_fail: false,
                    no_run: false,
                    should_panic: test::ShouldPanic::No,
                    test_type: test::TestType::UnitTest,
                },
                testfn: test::StaticTestFn(|| test::assert_test_result(
                    test_derive_simple(),
                )),
            };
            fn test_derive_simple() {
                init();
                let row = TestRow {
                    id: 1,
                    name: "nick".to_string(),
                    dt: OffsetDateTime::now_utc(),
                };
                let _row_values = row.db_values();
                {
                    use ::tracing::__macro_support::Callsite as _;
                    static CALLSITE: ::tracing::callsite::DefaultCallsite = {
                        static META: ::tracing::Metadata<'static> = {
                            ::tracing_core::metadata::Metadata::new(
                                "event src/schema/derive.rs:72",
                                "clickhouse_client::schema::derive::tests",
                                ::tracing::Level::INFO,
                                Some("src/schema/derive.rs"),
                                Some(72u32),
                                Some("clickhouse_client::schema::derive::tests"),
                                ::tracing_core::field::FieldSet::new(
                                    &["message"],
                                    ::tracing_core::callsite::Identifier(&CALLSITE),
                                ),
                                ::tracing::metadata::Kind::EVENT,
                            )
                        };
                        ::tracing::callsite::DefaultCallsite::new(&META)
                    };
                    let enabled = ::tracing::Level::INFO
                        <= ::tracing::level_filters::STATIC_MAX_LEVEL
                        && ::tracing::Level::INFO
                            <= ::tracing::level_filters::LevelFilter::current()
                        && {
                            let interest = CALLSITE.interest();
                            !interest.is_never()
                                && ::tracing::__macro_support::__is_enabled(
                                    CALLSITE.metadata(),
                                    interest,
                                )
                        };
                    if enabled {
                        (|value_set: ::tracing::field::ValueSet| {
                            let meta = CALLSITE.metadata();
                            ::tracing::Event::dispatch(meta, &value_set);
                        })({
                            #[allow(unused_imports)]
                            use ::tracing::field::{debug, display, Value};
                            let mut iter = CALLSITE.metadata().fields().iter();
                            CALLSITE
                                .metadata()
                                .fields()
                                .value_set(
                                    &[
                                        (
                                            &iter.next().expect("FieldSet corrupted (this is a bug)"),
                                            Some(&format_args!("TestRow OK") as &dyn Value),
                                        ),
                                    ],
                                )
                        });
                    } else {
                    }
                };
                {
                    ::std::io::_eprint(format_args!("{0:#?}\n", _row_values));
                };
            }
        }
    }
    mod types {
        //! Data types
        //!
        //! This modules provides the Clickhouse data types.
        //!
        //! Types are defined at [https://clickhouse.com/docs/en/sql-reference/data-types](https://clickhouse.com/docs/en/sql-reference/data-types).
        use std::fmt::Debug;
        /// Trait for Clickhouse types
        pub trait DbType: Debug {
            /// Converts a type to a SQL string
            fn to_sql_str(&self) -> String;
            /// Parses from a SQL string
            fn from_sql_str(s: &str) -> Result<Self, String>
            where
                Self: Sized;
        }
        /// Implements the nullable variant
        impl<T> DbType for Option<T>
        where
            T: DbType,
        {
            fn to_sql_str(&self) -> String {
                match self {
                    Some(value) => value.to_sql_str(),
                    None => "NULL".to_string(),
                }
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                T::from_sql_str(s).map(|x| Some(x))
            }
        }
        impl DbType for u8 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<u8>().map_err(|e| e.to_string())
            }
        }
        impl DbType for u16 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<u16>().map_err(|e| e.to_string())
            }
        }
        impl DbType for u32 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<u32>().map_err(|e| e.to_string())
            }
        }
        impl DbType for u64 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<u64>().map_err(|e| e.to_string())
            }
        }
        impl DbType for u128 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<u128>().map_err(|e| e.to_string())
            }
        }
        impl DbType for i8 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<i8>().map_err(|e| e.to_string())
            }
        }
        impl DbType for i16 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<i16>().map_err(|e| e.to_string())
            }
        }
        impl DbType for i32 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<i32>().map_err(|e| e.to_string())
            }
        }
        impl DbType for i64 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<i64>().map_err(|e| e.to_string())
            }
        }
        impl DbType for i128 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<i128>().map_err(|e| e.to_string())
            }
        }
        impl DbType for f32 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<f32>().map_err(|e| e.to_string())
            }
        }
        impl DbType for f64 {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<f64>().map_err(|e| e.to_string())
            }
        }
        impl DbType for bool {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<bool>().map_err(|e| e.to_string())
            }
        }
        impl DbType for String {
            fn to_sql_str(&self) -> String {
                {
                    let res = ::alloc::fmt::format(format_args!("\'{0}\'", self));
                    res
                }
            }
            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<String>().map_err(|e| e.to_string())
            }
        }
        #[cfg(feature = "time")]
        mod time {
            //! Data types extension for the `time` crate
            use ::time::{macros::format_description, OffsetDateTime, UtcOffset};
            use super::*;
            impl DbType for OffsetDateTime {
                fn to_sql_str(&self) -> String {
                    let date_utc = self.to_offset(UtcOffset::UTC);
                    let format = {
                        const DESCRIPTION: &[::time::format_description::FormatItem<
                            '_,
                        >] = &[
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Year({
                                    let mut value = ::time::format_description::modifier::Year::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                        .repr = ::time::format_description::modifier::YearRepr::Full;
                                    value.iso_week_based = false;
                                    value.sign_is_mandatory = false;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b"-",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Month({
                                    let mut value = ::time::format_description::modifier::Month::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                        .repr = ::time::format_description::modifier::MonthRepr::Numerical;
                                    value.case_sensitive = true;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b"-",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Day({
                                    let mut value = ::time::format_description::modifier::Day::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b" ",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Hour({
                                    let mut value = ::time::format_description::modifier::Hour::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value.is_12_hour_clock = false;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b":",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Minute({
                                    let mut value = ::time::format_description::modifier::Minute::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b":",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Second({
                                    let mut value = ::time::format_description::modifier::Second::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                        ];
                        DESCRIPTION
                    };
                    {
                        let res = ::alloc::fmt::format(
                            format_args!("\'{0}\'", date_utc.format(format).unwrap()),
                        );
                        res
                    }
                }
                fn from_sql_str(s: &str) -> Result<Self, String> {
                    let format = {
                        const DESCRIPTION: &[::time::format_description::FormatItem<
                            '_,
                        >] = &[
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Year({
                                    let mut value = ::time::format_description::modifier::Year::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                        .repr = ::time::format_description::modifier::YearRepr::Full;
                                    value.iso_week_based = false;
                                    value.sign_is_mandatory = false;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b"-",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Month({
                                    let mut value = ::time::format_description::modifier::Month::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                        .repr = ::time::format_description::modifier::MonthRepr::Numerical;
                                    value.case_sensitive = true;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b"-",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Day({
                                    let mut value = ::time::format_description::modifier::Day::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b" ",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Hour({
                                    let mut value = ::time::format_description::modifier::Hour::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value.is_12_hour_clock = false;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b":",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Minute({
                                    let mut value = ::time::format_description::modifier::Minute::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                            ::time::format_description::FormatItem::Literal {
                                0: b":",
                            },
                            ::time::format_description::FormatItem::Component {
                                0: ::time::format_description::Component::Second({
                                    let mut value = ::time::format_description::modifier::Second::default();
                                    value
                                        .padding = ::time::format_description::modifier::Padding::Zero;
                                    value
                                }),
                            },
                        ];
                        DESCRIPTION
                    };
                    OffsetDateTime::parse(s, format).map_err(|e| e.to_string())
                }
            }
        }
    }
    pub use derive::*;
    pub use types::*;
    /// Schema prelude
    pub mod prelude {
        pub use super::{ColSchema, DbRow, DbRowExt, DbSchema, DbType, TableSchema};
    }
    /// DB schema
    pub struct DbSchema {
        /// Database name
        pub db_name: String,
        /// Tables
        pub tables: HashMap<String, TableSchema>,
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for DbSchema {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field2_finish(
                f,
                "DbSchema",
                "db_name",
                &self.db_name,
                "tables",
                &&self.tables,
            )
        }
    }
    #[automatically_derived]
    impl ::core::default::Default for DbSchema {
        #[inline]
        fn default() -> DbSchema {
            DbSchema {
                db_name: ::core::default::Default::default(),
                tables: ::core::default::Default::default(),
            }
        }
    }
    impl DbSchema {
        /// Instantiates a new schema
        pub fn new(db_name: &str) -> Self {
            Self {
                db_name: db_name.to_string(),
                tables: HashMap::new(),
            }
        }
        /// Adds a table schema
        pub fn table(mut self, table: TableSchema) -> Self {
            self.tables.insert(table.name.clone(), table);
            self
        }
        /// Returns an immutable reference to a table schema
        pub fn get_table(&self, key: &str) -> Option<&TableSchema> {
            self.tables.get(key)
        }
        /// Returns a mutable reference to a table schema
        pub fn get_table_mut(&mut self, key: &str) -> Option<&mut TableSchema> {
            self.tables.get_mut(key)
        }
    }
    /// DB table schema
    pub struct TableSchema {
        /// Name
        pub name: String,
        /// Columns
        pub cols: HashMap<String, ColSchema>,
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for TableSchema {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field2_finish(
                f,
                "TableSchema",
                "name",
                &self.name,
                "cols",
                &&self.cols,
            )
        }
    }
    impl TableSchema {
        /// Instantiates
        pub fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                cols: HashMap::new(),
            }
        }
        /// Adds a column schema
        pub fn column(mut self, col: ColSchema) -> Self {
            self.cols.insert(col.name.clone(), col);
            self
        }
        /// Returns an immutable reference to a column schema
        pub fn get_column(&self, key: &str) -> Option<&ColSchema> {
            self.cols.get(key)
        }
        /// Returns a mutable reference to a column schema
        pub fn get_column_mut(&mut self, key: &str) -> Option<&mut ColSchema> {
            self.cols.get_mut(key)
        }
    }
    /// DB table schema
    pub struct ColSchema {
        /// Name
        pub name: String,
        /// Type (Clickhouse type)
        pub ty: String,
        /// Primary key
        pub is_primary: bool,
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for ColSchema {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field3_finish(
                f,
                "ColSchema",
                "name",
                &self.name,
                "ty",
                &self.ty,
                "is_primary",
                &&self.is_primary,
            )
        }
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Once;
    use tracing_ext::sub::PrettyConsoleLayer;
    use tracing_subscriber::{prelude::*, EnvFilter};
    static INIT: Once = Once::new();
    /// Initializes a tracer for unit tests
    pub fn init_test_tracer() {
        INIT.call_once(|| {
            let layer_pretty_stdout = PrettyConsoleLayer::default()
                .wrapped(true)
                .oneline(false)
                .events_only(false)
                .show_time(false)
                .show_target(true)
                .show_span_info(true)
                .indent(6);
            let filter_layer = EnvFilter::from_default_env();
            tracing_subscriber::registry()
                .with(layer_pretty_stdout)
                .with(filter_layer)
                .init();
        });
    }
}
#[rustc_main]
#[no_coverage]
pub fn main() -> () {
    extern crate test;
    test::test_main_static(
        &[
            &test_client_connect,
            &test_query_insert,
            &test_query_raw,
            &test_query_select,
            &test_derive_simple,
        ],
    )
}

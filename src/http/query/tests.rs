//! Query tests

use time::OffsetDateTime;
use tokio::sync::OnceCell;
use tracing::{error, info};

use super::*;
use crate::schema::prelude::*;

/// Test record
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

/// Tests that the schema is initialized
static INIT_SCHEMA: OnceCell<()> = OnceCell::const_new();

/// Returns a new client
fn new_client() -> Client {
    Client::new("http://localhost:8123").database("test")
}

/// Initializes the schema
#[tracing::instrument]
async fn init_tracer_schema() {
    INIT_SCHEMA
        .get_or_init(|| async {
            crate::tests::init_test_tracer();

            let client = new_client();
            let db_schema = Schema::new("test").table(<TestRecord as DbRowExt>::db_schema());
            client.create_db(&db_schema).await.unwrap();
            for (_, table_schema) in db_schema.tables {
                client.create_table(&table_schema).await.unwrap();
            }
        })
        .await;
}

#[tokio::test]
#[tracing::instrument]
async fn test_query_raw() {
    init_tracer_schema().await;
    let client = new_client();

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
    init_tracer_schema().await;
    let client = new_client();

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
    init_tracer_schema().await;
    let client = new_client();

    match client.select::<TestRecord>(&[], &Where::null()).await {
        Ok(_ok) => {
            info!("test_query_select OK");
        }
        Err(err) => {
            error!(%err, "test_query_select ERROR");
            panic!("{err}")
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_query_update() {
    init_tracer_schema().await;
    let client = new_client();

    let updated_record = TestRecord {
        id: 1,
        name: "test updated".to_string(),
        timestamp: OffsetDateTime::now_utc(),
        metric: 1.1,
        null_int: None,
    };

    match client
        .update::<TestRecord>(
            &updated_record,
            &["name"],
            &Where::new("id", "=", updated_record.id),
        )
        .await
    {
        Ok(_ok) => {
            info!("test_query_update OK");
        }
        Err(err) => {
            error!(%err, "test_query_update ERROR");
            panic!("{err}")
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_query_delete() {
    init_tracer_schema().await;
    let client = new_client();

    match client.delete::<TestRecord>(&Where::new("id", "=", 1)).await {
        Ok(_ok) => {
            info!("test_query_delete OK");
        }
        Err(err) => {
            error!(%err, "test_query_delete ERROR");
            panic!("{err}")
        }
    }
}

//! Tests

use crate::{orm::prelude::*, query::Where, schema::DbSchema, Client};

use time::OffsetDateTime;
use tokio::sync::OnceCell;

/// Test record
#[derive(Debug, DbRecord)]
#[db(table = "records")]
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

impl TestRecord {
    fn sample() -> Self {
        Self {
            id: 1,
            name: "name".to_string(),
            timestamp: OffsetDateTime::now_utc(),
            metric: 1.2,
            null_int: None,
        }
    }
}

static INIT: OnceCell<Client> = OnceCell::const_new();

// #[tracing::instrument]
async fn init() -> &'static Client {
    INIT.get_or_init(|| async {
        crate::tests::init_tracer();
        let client = Client::new("http://localhost:8123").database("test");
        let db_schema = DbSchema::new().table(TestRecord::db_schema());
        client.create_db("test", &db_schema).await.unwrap();
        for table_schema in db_schema.tables {
            client
                .create_table(&table_schema, "MergeTree()")
                .await
                .unwrap();
        }
        client
    })
    .await
}

#[tokio::test]
#[tracing::instrument]
async fn test_orm_derive() {
    init().await;

    let record = TestRecord::sample();
    let row_values = record.db_values();
    tracing::info!(?row_values, "test_derive OK");
}

#[tokio::test]
#[tracing::instrument]
async fn test_orm_insert() {
    let client = init().await;

    let record_1 = TestRecord::sample();
    let record_2 = TestRecord::sample();
    match client.insert(&[record_1, record_2]).await {
        Ok(_ok) => {
            tracing::info!("test_orm_insert OK");
        }
        Err(err) => {
            tracing::error!(%err, "test_orm_insert ERROR");
            panic!("{err}")
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_orm_select() {
    let client = init().await;

    match client.select::<TestRecord>(&[], Where::empty()).await {
        Ok(_ok) => {
            tracing::info!("test_orm_select OK");
        }
        Err(err) => {
            tracing::error!(%err, "test_orm_select ERROR");
            panic!("{err}")
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_orm_update() {
    let client = init().await;

    let updated_record = TestRecord {
        id: 1,
        name: "update name".to_string(),
        timestamp: OffsetDateTime::now_utc(),
        metric: 1.1,
        null_int: None,
    };

    match client
        .update::<TestRecord>(
            &updated_record,
            &["name"],
            Where::new("id", "=", updated_record.id),
        )
        .await
    {
        Ok(_ok) => {
            tracing::info!("test_orm_update OK");
        }
        Err(err) => {
            tracing::error!(%err, "test_orm_update ERROR");
            panic!("{err}")
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_orm_delete() {
    let client = init().await;

    match client.delete::<TestRecord>(Where::new("id", "=", 1)).await {
        Ok(_ok) => {
            tracing::info!("test_orm_delete OK");
        }
        Err(err) => {
            tracing::error!(%err, "test_orm_delete ERROR");
            panic!("{err}")
        }
    }
}

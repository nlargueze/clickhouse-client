//! Tests

use time::Date;
use tokio::{time::sleep, time::Duration};
use uuid::Uuid;

use std::str::FromStr;

use crate::query::Where;

use super::{prelude::*, ChRecord};

/// Test record
#[derive(Debug, Clone, AsChRecord)]
#[ch(table = "test_orm")]
struct TestRecord {
    /// ID
    #[ch(primary_key)]
    id: Uuid,
    /// Name
    name: String,
    /// Count
    count: u8,
    /// Date
    date: Date,
}

#[tokio::test]
#[tracing::instrument]
async fn orm_crud() {
    let client = crate::tests::init().await;

    client.orm::<TestRecord>().drop_table().await.unwrap();
    client
        .orm::<TestRecord>()
        .create_table("MergeTree()")
        .await
        .unwrap();

    let mut sample = TestRecord {
        id: Uuid::from_str("a0fe6bf1-1302-4e69-bfde-152a2f4d0e93").unwrap(),
        name: "name".to_string(),
        count: 1,
        date: Date::from_ordinal_date(2023, 1).unwrap(),
    };

    client.orm().insert(vec![sample.clone()]).await.unwrap();

    sample.name = "name 2".to_string();
    sample.count = 10;
    client
        .orm()
        .update_one(sample.clone(), vec![])
        .await
        .unwrap();

    // NB: dummy delay to allow the update to be made before the select query
    sleep(Duration::from_millis(10)).await;

    let res = client
        .orm::<TestRecord>()
        .select(Some(Where::with_val("id", "=", sample.id)))
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].count, 10);

    client.orm().delete(vec![sample]).await.unwrap();
}

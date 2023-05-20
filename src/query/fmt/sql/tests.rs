//! Tests

use crate::ty::time::{AsDate32, AsDateTime64};

#[tokio::test]
async fn test_query_sql_uuid() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE uuid = ??")
        .bind(::uuid::Uuid::new_v4())
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_sql_int() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE uint8 = ??")
        .bind(1)
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_sql_string() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE string = ??")
        .bind("abc")
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_sql_date() {
    let client = crate::tests::init().await;
    let date = time::Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
    client
        .query("SELECT * FROM tests WHERE date = ??")
        .bind(date.as_date32())
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_sql_date32() {
    let client = crate::tests::init().await;
    let date = time::Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
    client
        .query("SELECT * FROM tests WHERE date32 = ??")
        .bind(date.as_date32())
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic]
async fn test_query_sql_datetime() {
    let client = crate::tests::init().await;
    let date = time::OffsetDateTime::now_utc();
    client
        .query("SELECT * FROM tests WHERE datetime = ??")
        .bind(date.as_datetime64())
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_sql_datetime64() {
    let client = crate::tests::init().await;
    let date = time::OffsetDateTime::now_utc();
    client
        .query("SELECT * FROM tests WHERE datetime64 = ??")
        .bind(date.as_datetime64())
        .exec()
        .await
        .unwrap();
}

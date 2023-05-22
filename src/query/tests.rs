//! Query tests

#[tokio::test]
async fn query_bind_str() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE uuid = ??")
        .bind("6f2f0129-7956-4d73-80b8-1860fbe1121a")
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn query_bind_uuid() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE uuid = ??")
        .bind(::uuid::Uuid::new_v4())
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn query_bind_int() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE uint8 = ??")
        .bind(1)
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
async fn query_bind_string() {
    let client = crate::tests::init().await;
    client
        .query("SELECT * FROM tests WHERE string = ??")
        .bind("abc")
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "time")]
async fn query_bind_date() {
    let client = crate::tests::init().await;
    let date = time::Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
    client
        .query("SELECT * FROM tests WHERE date = ??")
        .bind(date)
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "time")]
async fn query_bind_date32() {
    let client = crate::tests::init().await;
    let date = time::Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
    client
        .query("SELECT * FROM tests WHERE date32 = ??")
        .bind(date)
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "time")]
#[should_panic]
async fn query_bind_datetime() {
    let client = crate::tests::init().await;
    let date = time::OffsetDateTime::now_utc();
    client
        .query("SELECT * FROM tests WHERE datetime = ??")
        .bind(date)
        .exec()
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "time")]
async fn query_bind_datetime64() {
    let client = crate::tests::init().await;
    let date = time::OffsetDateTime::now_utc();
    client
        .query("SELECT * FROM tests WHERE datetime64 = ??")
        .bind(date)
        .exec()
        .await
        .unwrap();
}

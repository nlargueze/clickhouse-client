//! Tests

use super::SqlSerializer;
use serde::Serialize;

#[tokio::test]
#[cfg(feature = "uuid")]
async fn test_fmt_sql_uuid() {
    use uuid::Uuid;

    let _ = crate::tests::init().await;
    let serializer = SqlSerializer::new();

    let uuid_str = "f753a6d7-5415-420e-ace2-711b000ac5a5";
    let uuid = Uuid::parse_str(uuid_str).unwrap();
    let uuid_ser = uuid.serialize(serializer).unwrap();
    assert_eq!(uuid_ser, format!("'{uuid_str}'"));
}

#[tokio::test]
async fn test_fmt_sql_u32() {
    let _ = crate::tests::init().await;
    let serializer = SqlSerializer::new();

    let u = 10_u32;
    let u_ser = u.serialize(serializer).unwrap();
    assert_eq!(u_ser, u.to_string());
}

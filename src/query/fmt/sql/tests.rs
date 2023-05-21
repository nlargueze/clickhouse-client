//! Tests

use crate::orm::Value;

use super::SqlSerializer;
use serde::Serialize;

#[test]
fn test_fmt_sql_u8() {
    let serializer = SqlSerializer::new();
    let x: Value = 1u8.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq!(x_ser, "1");
}

#[test]
fn test_fmt_sql_str() {
    let serializer = SqlSerializer::new();
    let x: Value = "abcd".into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq!(x_ser, "'abcd'");
}

#[test]
#[cfg(feature = "uuid")]
fn test_fmt_sql_uuid() {
    use uuid::Uuid;

    let serializer = SqlSerializer::new();
    let id = Uuid::parse_str("f753a6d7-5415-420e-ace2-711b000ac5a5").unwrap();
    let x: Value = id.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq!(x_ser, "'f753a6d7-5415-420e-ace2-711b000ac5a5'");
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_sql_date() {
    use crate::orm::time::AsDate32;
    use time::{Date, Month};

    let serializer = SqlSerializer::new();
    let date = Date::from_calendar_date(1970, Month::January, 1).unwrap();
    let x = date.as_date32();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq!(x_ser, "'1970-01-01'");
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_sql_datetime() {
    use crate::orm::time::AsDateTime64;
    use time::{Month, OffsetDateTime};

    let serializer = SqlSerializer::new();
    let dt = OffsetDateTime::from_unix_timestamp(0).unwrap();
    let x = dt.as_datetime64();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq!(x_ser, "'1970-01-01 00:00:00.0'");
}

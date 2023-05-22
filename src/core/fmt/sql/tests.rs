//! Tests

use crate::core::Value;

use super::SqlFormatter;

#[test]
fn test_fmt_sql_u8() {
    let formatter = SqlFormatter::new();
    let x: Value = 1u8.into();
    let x_ser = x.format(&formatter);
    assert_eq!(x_ser, "1");
}

#[test]
fn test_fmt_sql_str() {
    let formatter = SqlFormatter::new();
    let x: Value = "abcd".into();
    let x_ser = x.format(&formatter);
    assert_eq!(x_ser, "'abcd'");
}

#[test]
#[cfg(feature = "uuid")]
fn test_fmt_sql_uuid() {
    use uuid::Uuid;

    let formatter = SqlFormatter::new();
    let id = Uuid::parse_str("f753a6d7-5415-420e-ace2-711b000ac5a5").unwrap();
    let x: Value = id.into();
    let x_ser = x.format(&formatter);
    assert_eq!(x_ser, "'f753a6d7-5415-420e-ace2-711b000ac5a5'");
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_sql_date() {
    use time::{Date, Month};

    let formatter = SqlFormatter::new();
    let date = Date::from_calendar_date(1970, Month::January, 1).unwrap();
    let x: Value = date.into();
    let x_ser = x.format(&formatter);
    assert_eq!(x_ser, "'1970-01-01'");
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_sql_datetime() {
    use time::OffsetDateTime;

    let formatter = SqlFormatter::new();
    let dt = OffsetDateTime::from_unix_timestamp(0).unwrap();
    let x: Value = dt.into();
    let x_ser = x.format(&formatter);
    assert_eq!(x_ser, "'1970-01-01 00:00:00.0'");
}

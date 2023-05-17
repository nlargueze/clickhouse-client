//! Tests for Value

use ethnum::U256;
use time::{Date, Month, OffsetDateTime};
use uuid::Uuid;

use crate::value::ChValue;

#[test]
fn sql_u8() {
    let value = 1u8.into_ch_value();
    assert_eq!(value.to_sql_string(), "1");
}

#[test]
fn sql_u256() {
    let value = U256::from_words(1, 1).into_ch_value();
    assert_eq!(
        value.to_sql_string(),
        "340282366920938463463374607431768211457"
    );
}

#[test]
fn sql_f32() {
    let value = 1.123_f32.into_ch_value();
    assert_eq!(value.to_sql_string(), "1.123");
}

#[test]
fn sql_bool() {
    let value = true.into_ch_value();
    assert_eq!(value.to_sql_string(), "1");

    let value = false.into_ch_value();
    assert_eq!(value.to_sql_string(), "0");
}

#[test]
fn sql_string() {
    let value = "abcd".into_ch_value();
    assert_eq!(value.to_sql_string(), "'abcd'");
}

#[test]
fn sql_uuid() {
    let value = Uuid::parse_str("f753a6d7-5415-420e-ace2-711b000ac5a5")
        .unwrap()
        .into_ch_value();
    assert_eq!(
        value.to_sql_string(),
        "'f753a6d7-5415-420e-ace2-711b000ac5a5'"
    );
}

#[test]
fn sql_date() {
    let value = Date::from_calendar_date(1970, Month::January, 30)
        .unwrap()
        .into_ch_value();
    assert_eq!(value.to_sql_string(), "'1970-01-30'");
}

#[test]
fn sql_datetime() {
    let value = OffsetDateTime::from_unix_timestamp(0)
        .unwrap()
        .into_ch_value();
    assert_eq!(value.to_sql_string(), "'1970-01-01 00:00:00.0'");
}

#[test]
fn sql_array() {
    let value = vec![1_u8, 2, 3].into_ch_value();
    assert_eq!(value.to_sql_string(), "[1, 2, 3]");
}

#[test]
fn sql_tuple() {
    let value = (1_u8, 2, 3).into_ch_value();
    assert_eq!(value.to_sql_string(), "(1, 2, 3)");
}

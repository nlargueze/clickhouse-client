//! Query tests

use std::str::FromStr;

use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::query::Query;

#[test]
fn query_bind_str() {
    let query = Query::new("SELECT * FROM tests WHERE string = [??]").bind_val("abc");
    assert_eq!(query.statement, "SELECT * FROM tests WHERE string = 'abc'");
}

#[test]
fn query_bind_uuid() {
    let uuid = Uuid::from_str("00fcec51-7871-437c-aa38-e65225ea814b").unwrap();
    let query = Query::new("SELECT * FROM tests WHERE string = [??]").bind_val(uuid);
    assert_eq!(
        query.statement,
        "SELECT * FROM tests WHERE string = '00fcec51-7871-437c-aa38-e65225ea814b'"
    );
}

#[test]
fn query_bind_int() {
    let query = Query::new("SELECT * FROM tests WHERE uint8 = [??]").bind_val(1);
    assert_eq!(query.statement, "SELECT * FROM tests WHERE uint8 = 1");
}

#[test]
fn query_bind_date() {
    let date = Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
    let query = Query::new("SELECT * FROM tests WHERE date = [??]").bind_val(date);
    assert_eq!(
        query.statement,
        "SELECT * FROM tests WHERE date = '1970-01-01'"
    );
}

#[test]
fn query_bind_datetime() {
    let dt = OffsetDateTime::from_unix_timestamp_nanos(0).unwrap();
    let query = Query::new("SELECT * FROM tests WHERE datetime = [??]").bind_val(dt);
    assert_eq!(
        query.statement,
        "SELECT * FROM tests WHERE datetime = '1970-01-01 00:00:00.0'"
    );
}

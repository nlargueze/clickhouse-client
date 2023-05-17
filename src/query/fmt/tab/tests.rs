//! Tests

use core::str::FromStr;
use std::collections::HashMap;

use ethnum::{I256, U256};
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::{
    query::{Format, QueryData, TsvFormatter},
    value::{time::DateExt, ChValue, Type},
};

/// Sets a test
macro_rules! set_test {
    // `()` indicates that the macro takes no argument.
    ($ID:ident, $TY:ty, $VAL:expr, $STR:literal) => {
        #[test]
        fn $ID() {
            let x: $TY = $VAL;
            let ty = <$TY as ChValue>::ch_type();
            let value = x.into_ch_value();
            let formatter = TsvFormatter::default();
            let value_str = formatter.format_value(value.clone());
            assert_eq!(value_str, $STR);

            let value_parsed = formatter.parse_value(&value_str, ty).unwrap();
            assert_eq!(value_parsed, value);
        }
    };
}

set_test!(fmt_tsv_uint8, u8, 1, "1");
set_test!(
    fmt_tsv_uint256,
    U256,
    U256::from_words(1, 1),
    "340282366920938463463374607431768211457"
);
set_test!(fmt_tsv_int256, I256, I256::from_words(-1, -1), "-1");
set_test!(fmt_tsv_f32, f32, 1.123, "1.123");
set_test!(fmt_tsv_f64, f64, 1.123, "1.123");
set_test!(fmt_tsv_bool, bool, true, "true");
set_test!(
    fmt_tsv_string,
    String,
    "hello world".to_string(),
    r"hello\nworld"
);
set_test!(
    fmt_tsv_string_2,
    String,
    "with\\backslash".to_string(),
    r"with\bbackslash"
);
set_test!(
    fmt_tsv_uuid,
    Uuid,
    Uuid::from_str("0e8cfb2e-3777-4c6e-876e-42537ee7f9ba").unwrap(),
    "0e8cfb2e-3777-4c6e-876e-42537ee7f9ba"
);
set_test!(
    fmt_tsv_date,
    Date,
    Date::from_unix_days(1).unwrap(),
    "1970-01-02"
);
set_test!(
    fmt_tsv_datetime,
    OffsetDateTime,
    OffsetDateTime::from_unix_timestamp(1).unwrap(),
    "1970-01-01 00:00:01.0"
);
set_test!(fmt_tsv_array, Vec<u8>, vec![0, 1, 2], "[0, 1, 2]");
set_test!(fmt_tsv_tuple, (u8, u8, u8), (1, 2, 3), "(1, 2, 3)");
set_test!(
    fmt_tsv_map,
    HashMap<String, u8>,
    HashMap::from([("key0".to_string(), 0_u8), ("key1".to_string(), 1_u8)]),
    "{'key0': 0, 'key1': 1}"
);
set_test!(fmt_tsv_uint8_null_some, Option<u8>, Some(1), "1");
set_test!(fmt_tsv_uint8_null_none, Option<u8>, None, r"\N");

/// Creates a sample table
fn sample_table() -> QueryData {
    QueryData::with_names_and_types(vec![
        ("u8", Type::UInt8),
        ("i8", Type::Int8),
        ("u256", Type::UInt256),
        ("i256", Type::Int256),
        ("f32", Type::Float32),
        ("f64", Type::Float64),
        ("bool", Type::Bool),
        ("string", Type::String),
        ("uuid", Type::UUID),
        ("date", Type::Date32),
        ("datetime", Type::DateTime64(9)),
        ("array", Type::Array(Box::new(Type::UInt8))),
    ])
    .row(vec![
        1_u8.into(),
        (-1_i8).into(),
        U256::from_words(1, 1).into(),
        I256::from_words(-1, 1).into(),
        1.123_f32.into(),
        1.123_f64.into(),
        true.into(),
        "hello world".into(),
        Uuid::new_v4().into(),
        Date::from_unix_days(1).unwrap().into(),
        OffsetDateTime::now_utc().into(),
        vec![1_u8, 2_u8].into(),
    ])
}

#[test]
fn fmt_tsv_table() {
    let table = sample_table();
    // eprintln!("{table}");
    let format = Format::TabSepWithNamesAndTypes;
    let bytes = table.clone().to_bytes(format).unwrap();
    // let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    // eprintln!("{bytes_str}");

    let table_parsed = QueryData::from_bytes(&bytes, format, None).unwrap();
    assert_eq!(table_parsed, table);
}

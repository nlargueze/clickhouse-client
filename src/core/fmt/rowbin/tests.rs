//! Tests

use std::io::Cursor;

use assert_hex::assert_eq_hex;

use crate::{
    core::{
        fmt::{rowbin::RowBinFormatter, TableFormatter},
        Type, Value,
    },
    query::QueryTable,
};

#[test]
fn test_fmt_rowbin_u8() {
    let formatter = RowBinFormatter::new();
    let x: Value = 1u8.into();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, vec![0x01]);

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::UInt8).unwrap();
    assert_eq!(x_de, x);
}

#[test]
fn test_fmt_rowbin_u32() {
    let formatter = RowBinFormatter::new();
    let x: Value = 0xAB_CD_EF_u32.into();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, vec![0xEF, 0xCD, 0xAB, 0x00]);

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::UInt32).unwrap();
    assert_eq!(x_de, x);
}

#[test]
fn test_fmt_rowbin_bool() {
    let formatter = RowBinFormatter::new();
    let x: Value = true.into();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, vec![0x01]);

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::Bool).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[should_panic]
fn test_fmt_rowbin_bool_err() {
    let formatter = RowBinFormatter::new();

    let x_ser = vec![0x02];
    let mut reader = Cursor::new(x_ser);
    let _x_de = Value::parse(&formatter, &mut reader, Type::Bool).unwrap();
}

#[test]
fn test_fmt_rowbin_str() {
    let formatter = RowBinFormatter::new();

    let x: Value = "abcd".into();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, vec![0x04, 0x61, 0x62, 0x63, 0x64]);

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::String).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "uuid")]
fn test_fmt_rowbin_uuid() {
    use ::uuid::Uuid;

    let formatter = RowBinFormatter::new();

    let id = Uuid::parse_str("f753a6d7-5415-420e-ace2-711b000ac5a5").unwrap();
    let x: Value = id.into();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, id.into_bytes());

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::UUID).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_rowbin_date() {
    use crate::core::time::AsDate32;
    use ::time::Date;
    use time::Month;

    let formatter = RowBinFormatter::new();

    let date = Date::from_calendar_date(1970, Month::January, 11).unwrap();
    let days_since_epoch =
        (date - (Date::from_calendar_date(1970, Month::January, 1).unwrap())).whole_days() as i32;
    let x: Value = date.as_date32();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, days_since_epoch.to_le_bytes());

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::Date32).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_rowbin_datetime() {
    use crate::core::time::AsDateTime64;
    use ::time::OffsetDateTime;

    let formatter = RowBinFormatter::new();

    let date = OffsetDateTime::now_utc();
    let x: Value = date.as_datetime64();
    let x_ser = x.format(&formatter);
    assert_eq_hex!(x_ser, (date.unix_timestamp_nanos() as i64).to_le_bytes());

    let mut reader = Cursor::new(x_ser);
    let x_de = Value::parse(&formatter, &mut reader, Type::DateTime64(9)).unwrap();
    assert_eq!(x_de, x);
}

#[test]
fn test_fmt_rowbin_table() {
    let table = QueryTable {
        names: vec!["id".to_string(), "name".to_string()],
        types: vec![Type::UInt8, Type::String],
        rows: vec![
            vec![1_u8.into(), "abcd".into()],
            vec![2_u8.into(), "efgh".into()],
        ],
    };
    let formatter = RowBinFormatter::new_with_names_and_types();
    let bytes = formatter.format_table(&table);

    let table_de = formatter.parse_table(&mut bytes.as_slice(), None).unwrap();
    assert_eq!(table_de.names.len(), 2);
}

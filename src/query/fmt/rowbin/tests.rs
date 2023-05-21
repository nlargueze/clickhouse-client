//! Tests

use assert_hex::assert_eq_hex;
use serde::{Deserialize, Serialize};

use crate::orm::{Type, Value};

use super::{RowBinDeserializer, RowBinSerializer};

#[test]
fn test_fmt_rowbin_u8() {
    let serializer = RowBinSerializer::new();
    let x: Value = 1u8.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, vec![0x01]);

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::UInt8).unwrap();
    assert_eq!(x_de, x);
}

#[test]
fn test_fmt_rowbin_u32() {
    let serializer = RowBinSerializer::new();
    let x: Value = 0xAB_CD_EF_u32.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, vec![0xEF, 0xCD, 0xAB, 0x00]);

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::UInt32).unwrap();
    assert_eq!(x_de, x);
}

#[test]
fn test_fmt_rowbin_bool() {
    let serializer = RowBinSerializer::new();
    let x: Value = true.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, vec![0x01]);

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::Bool).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[should_panic]
fn test_fmt_rowbin_bool_err() {
    let x_ser = vec![0x02];
    let deserializer = RowBinDeserializer::new(&x_ser);
    let _x = bool::deserialize(deserializer).unwrap();
}

#[test]
fn test_fmt_rowbin_str() {
    let serializer = RowBinSerializer::new();
    let x: Value = "abcd".into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, vec![0x04, 0x61, 0x62, 0x63, 0x64]);

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::String).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "uuid")]
fn test_fmt_rowbin_uuid() {
    use ::uuid::Uuid;

    let serializer = RowBinSerializer::new();
    let id = Uuid::parse_str("f753a6d7-5415-420e-ace2-711b000ac5a5").unwrap();
    let x: Value = id.into();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, id.into_bytes());

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::UUID).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_rowbin_date() {
    use ::time::Date;
    use time::Month;

    use crate::orm::time::AsDate32;

    let serializer = RowBinSerializer::new();
    let date = Date::from_calendar_date(1970, Month::January, 11).unwrap();
    let days_since_epoch =
        (date - (Date::from_calendar_date(1970, Month::January, 1).unwrap())).whole_days() as i32;
    let x: Value = date.as_date32();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, days_since_epoch.to_le_bytes());

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::Date32).unwrap();
    assert_eq!(x_de, x);
}

#[test]
#[cfg(feature = "time")]
fn test_fmt_rowbin_datetime() {
    use ::time::OffsetDateTime;

    use crate::orm::time::AsDateTime64;

    let serializer = RowBinSerializer::new();
    let date = OffsetDateTime::now_utc();
    let x: Value = date.as_datetime64();
    let x_ser = x.serialize(serializer).unwrap();
    assert_eq_hex!(x_ser, (date.unix_timestamp_nanos() as i64).to_le_bytes());

    let deserializer = RowBinDeserializer::new(&x_ser);
    let x_de = Value::deserialize_type(deserializer, Type::DateTime64(9)).unwrap();
    assert_eq!(x_de, x);
}

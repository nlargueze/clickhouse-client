//! Format tests

use super::{rowbin::RowBinSerializer, sql::SqlSerializer, Value};
use serde::Serialize;

#[tokio::test]
async fn test_fmt_value_u8() {
    let u = Value::UInt8(4);

    let serializer = SqlSerializer::new();
    let u_ser = u.serialize(serializer).unwrap();
    assert_eq!(u_ser, "4");

    let serializer = RowBinSerializer::new();
    let u_ser = u.serialize(serializer).unwrap();
    assert_eq!(u_ser, vec![0x04]);
}

#[tokio::test]
async fn test_fmt_value_str() {
    let s = Value::String("abcd".to_string());

    let serializer = SqlSerializer::new();
    let s_ser = s.serialize(serializer).unwrap();
    assert_eq!(s_ser, "'abcd'");

    let serializer = RowBinSerializer::new();
    let s_ser = s.serialize(serializer).unwrap();
    assert_eq!(s_ser, [0x04, 0x61, 0x62, 0x63, 0x64]);
}

//! Tests

use assert_hex::assert_eq_hex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{de::RowBinDeserializer, ser::RowBinSerializer};

#[tokio::test]
async fn test_query_fmt_rowbin_ser_uuid() {
    let serializer = RowBinSerializer::default();
    let uuid = Uuid::new_v4();
    let bytes = uuid.serialize(serializer).unwrap();
    assert_eq_hex!(bytes, uuid.as_bytes());
}

#[tokio::test]
async fn test_query_fmt_rowbin_ser_u32() {
    let i = 100_000_u32;
    let serializer = RowBinSerializer::default();
    let bytes = i.serialize(serializer).unwrap();
    assert_eq_hex!(bytes, i.to_le_bytes());
}

#[tokio::test]
async fn test_query_fmt_rowbin_ser_str() {
    let s = "abcd";
    let serializer = RowBinSerializer::default();
    let bytes = s.serialize(serializer).unwrap();
    // eprintln!("{:0X?}", s.as_bytes());
    // eprintln!("{bytes:0X?}");
    let mut s_with_prefix = vec![4];
    s_with_prefix.append(&mut s.to_string().as_bytes().to_vec());
    assert_eq_hex!(bytes, s_with_prefix);
}

#[tokio::test]
async fn test_query_fmt_rowbin_de_uuid() {
    let value = Uuid::new_v4();
    let bytes = value.to_u128_le().to_le_bytes();
    let deserializer = RowBinDeserializer::new(&bytes);
    let uuid = Uuid::deserialize(deserializer).unwrap();
    assert_eq!(uuid, value)
}

#[tokio::test]
async fn test_query_fmt_rowbin_de_str() {
    let value = "abcd";
    let value_b = value.as_bytes();
    let bytes: Vec<u8> = [&[0x04], value_b].concat();
    let deserializer = RowBinDeserializer::new(&bytes);
    let s = String::deserialize(deserializer).unwrap();
    assert_eq!(s, value)
}

#[tokio::test]
async fn test_query_fmt_rowbin_de_u16() {
    let value = 1_000_u16;
    let bytes = value.to_le_bytes();
    let deserializer = RowBinDeserializer::new(&bytes);
    let i = u16::deserialize(deserializer).unwrap();
    assert_eq!(i, value)
}

#[tokio::test]
async fn test_query_fmt_rowbin_de_bool() {
    let value = true;
    let bytes = [value as u8];
    let deserializer = RowBinDeserializer::new(&bytes);
    let b = bool::deserialize(deserializer).unwrap();
    assert_eq!(b, value)
}

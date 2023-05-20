//! Tests

use super::Type;

#[tokio::test]
async fn test_ty_uint8() {
    let ty = Type::UInt8;
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "UInt8");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::UInt8);
}

#[tokio::test]
async fn test_ty_uint8_null() {
    let ty = Type::NullableUInt8;
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Nullable(UInt8)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::NullableUInt8);
}

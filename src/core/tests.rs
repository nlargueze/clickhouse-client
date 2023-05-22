//! Tests

use super::ty::Type;

#[test]
fn test_type_uint8() {
    let ty = Type::UInt8;
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "UInt8");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::UInt8);
}

#[test]
fn test_type_uint8_null() {
    let ty = Type::NullableUInt8;
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Nullable(UInt8)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::NullableUInt8);
}

#[test]
fn test_type_dec() {
    let ty = Type::Decimal(6, 2);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Decimal(6,2)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Decimal(6, 2));
}

#[test]
fn test_type_dec32() {
    let ty = Type::Decimal32(4);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Decimal32(4)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Decimal32(4));
}

#[test]
fn test_type_dec64() {
    let ty = Type::Decimal64(4);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Decimal64(4)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Decimal64(4));
}

#[test]
fn test_type_dec128() {
    let ty = Type::Decimal128(4);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Decimal128(4)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Decimal128(4));
}

#[test]
fn test_type_dec256() {
    let ty = Type::Decimal256(4);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Decimal256(4)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Decimal256(4));
}

#[test]
fn test_type_enum() {
    let ty = Type::Enum8(vec![
        ("variant_1".into(), Some(1)),
        ("variant_2".into(), Some(2)),
    ]);
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Enum8('variant_1' = 1, 'variant_2' = 2)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, ty);
}

#[test]
fn test_type_array() {
    let ty = Type::Array(Box::new(Type::UInt8));
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Array(UInt8)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(ty_parsed, Type::Array(Box::new(Type::UInt8)));
}

#[test]
fn test_type_map() {
    let ty = Type::Map(Box::new(Type::String), Box::new(Type::UInt8));
    let ty_str = ty.to_string();
    assert_eq!(ty_str, "Map(String, UInt8)");

    let ty_parsed: Type = ty_str.parse().unwrap();
    assert_eq!(
        ty_parsed,
        Type::Map(Box::new(Type::String), Box::new(Type::UInt8))
    );
}

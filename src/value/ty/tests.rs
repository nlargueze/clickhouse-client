//! Type tests

use std::collections::BTreeMap;

use super::Type;

/// Sets a test
macro_rules! set_test {
    // `()` indicates that the macro takes no argument.
    ($ID:ident, $TY:expr, $TYSTR:literal) => {
        #[test]
        fn $ID() {
            let ty = $TY;
            let ty_str = ty.to_string();
            assert_eq!(ty_str, $TYSTR);

            let ty_parsed = ty_str.parse::<Type>().unwrap();
            assert_eq!(ty_parsed, ty);
        }
    };
}

set_test!(type_str_uint8, Type::UInt8, "UInt8");
set_test!(type_str_uint16, Type::UInt16, "UInt16");
set_test!(type_str_uint32, Type::UInt32, "UInt32");
set_test!(type_str_uint64, Type::UInt64, "UInt64");
set_test!(type_str_uint128, Type::UInt128, "UInt128");
set_test!(type_str_uint256, Type::UInt256, "UInt256");
set_test!(type_str_int8, Type::Int8, "Int8");
set_test!(type_str_int16, Type::Int16, "Int16");
set_test!(type_str_int32, Type::Int32, "Int32");
set_test!(type_str_int64, Type::Int64, "Int64");
set_test!(type_str_int128, Type::Int128, "Int128");
set_test!(type_str_int256, Type::Int256, "Int256");
set_test!(type_str_float32, Type::Float32, "Float32");
set_test!(type_str_float64, Type::Float64, "Float64");
set_test!(type_str_decimal, Type::Decimal(1, 1), "Decimal(1,1)");
set_test!(type_str_decimal32, Type::Decimal32(1), "Decimal32(1)");
set_test!(type_str_decimal64, Type::Decimal64(1), "Decimal64(1)");
set_test!(type_str_decimal128, Type::Decimal128(1), "Decimal128(1)");
set_test!(type_str_decimal256, Type::Decimal256(1), "Decimal256(1)");
set_test!(type_str_bool, Type::Bool, "Bool");
set_test!(type_str_string, Type::String, "String");
set_test!(
    type_str_fixed_string,
    Type::FixedString(1),
    "FixedString(1)"
);
set_test!(type_str_uuid, Type::UUID, "UUID");
set_test!(type_str_date, Type::Date, "Date");
set_test!(type_str_date32, Type::Date32, "Date32");
set_test!(type_str_datetime, Type::DateTime, "DateTime");
set_test!(type_str_datetime64, Type::DateTime64(1), "DateTime64(1)");
set_test!(
    type_str_enum8,
    Type::Enum8(BTreeMap::from([
        ("var1".to_string(), 0),
        ("var2".to_string(), 1),
    ])),
    "Enum8('var1' = 0, 'var2' = 1)"
);
set_test!(
    type_str_enum16,
    Type::Enum16(BTreeMap::from([
        ("var1".to_string(), 1),
        ("var2".to_string(), 2),
    ])),
    "Enum16('var1' = 1, 'var2' = 2)"
);
set_test!(
    type_str_array,
    Type::Array(Box::new(Type::UInt8)),
    "Array(UInt8)"
);
set_test!(
    type_str_tuple,
    Type::Tuple(vec![Type::UInt8, Type::UInt16]),
    "Tuple(UInt8, UInt16)"
);
set_test!(
    type_str_map,
    Type::Map(Box::new(Type::String), Box::new(Type::UInt8)),
    "Map(String, UInt8)"
);
set_test!(
    type_str_nested,
    Type::Nested(vec![
        ("a".to_string(), Type::UInt8),
        ("b".to_string(), Type::UInt16),
    ]),
    "Nested(a UInt8, b UInt16)"
);
set_test!(type_str_uint8_null, Type::NullableUInt8, "Nullable(UInt8)");
set_test!(
    type_str_uint16_null,
    Type::NullableUInt16,
    "Nullable(UInt16)"
);
set_test!(
    type_str_uint32_null,
    Type::NullableUInt32,
    "Nullable(UInt32)"
);
set_test!(
    type_str_uint64_null,
    Type::NullableUInt64,
    "Nullable(UInt64)"
);
set_test!(
    type_str_uint128_null,
    Type::NullableUInt128,
    "Nullable(UInt128)"
);
set_test!(
    type_str_uint256_null,
    Type::NullableUInt256,
    "Nullable(UInt256)"
);
set_test!(type_str_int8_null, Type::NullableInt8, "Nullable(Int8)");
set_test!(type_str_int16_null, Type::NullableInt16, "Nullable(Int16)");
set_test!(type_str_int32_null, Type::NullableInt32, "Nullable(Int32)");
set_test!(type_str_int64_null, Type::NullableInt64, "Nullable(Int64)");
set_test!(
    type_str_int128_null,
    Type::NullableInt128,
    "Nullable(Int128)"
);
set_test!(
    type_str_int256_null,
    Type::NullableInt256,
    "Nullable(Int256)"
);
set_test!(
    type_str_float32_null,
    Type::NullableFloat32,
    "Nullable(Float32)"
);
set_test!(
    type_str_float64_null,
    Type::NullableFloat64,
    "Nullable(Float64)"
);
set_test!(
    type_str_decimal_null,
    Type::NullableDecimal(1, 1),
    "Nullable(Decimal(1,1))"
);
set_test!(
    type_str_decimal32_null,
    Type::NullableDecimal32(1),
    "Nullable(Decimal32(1))"
);
set_test!(
    type_str_decimal64_null,
    Type::NullableDecimal64(1),
    "Nullable(Decimal64(1))"
);
set_test!(
    type_str_decimal128_null,
    Type::NullableDecimal128(1),
    "Nullable(Decimal128(1))"
);
set_test!(
    type_str_decimal256_null,
    Type::NullableDecimal256(1),
    "Nullable(Decimal256(1))"
);
set_test!(type_str_bool_null, Type::NullableBool, "Nullable(Bool)");
set_test!(
    type_str_string_null,
    Type::NullableString,
    "Nullable(String)"
);
set_test!(
    type_str_fixed_string_null,
    Type::NullableFixedString(1),
    "Nullable(FixedString(1))"
);
set_test!(type_str_uuid_null, Type::NullableUUID, "Nullable(UUID)");
set_test!(type_str_date_null, Type::NullableDate, "Nullable(Date)");
set_test!(
    type_str_date32_null,
    Type::NullableDate32,
    "Nullable(Date32)"
);
set_test!(
    type_str_datetime_null,
    Type::NullableDateTime,
    "Nullable(DateTime)"
);
set_test!(
    type_str_datetime64_null,
    Type::NullableDateTime64(1),
    "Nullable(DateTime64(1))"
);

//! Tests for Value

use std::{collections::HashMap, str::FromStr};

use ethnum::{I256, U256};
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use super::ChValue;

/// Sets a test
macro_rules! set_test {
    // `()` indicates that the macro takes no argument.
    ($ID:ident, $TY:ty, $VAL:expr) => {
        #[test]
        fn $ID() {
            let value: $TY = $VAL;
            let value_ch = value.clone().into_ch_value();
            let value_parsed = value_ch.try_into::<$TY>().unwrap();
            assert_eq!(value, value_parsed);
        }
    };
}

set_test!(value_uint8, u8, 1);
set_test!(value_uint16, u16, 300);
set_test!(value_uint32, u32, 300000);
set_test!(value_uint64, u64, 300000);
set_test!(value_uint128, u128, 300000);
set_test!(
    value_uint256,
    U256,
    U256::from_str("11111111111111111111111111111111111111111111111111111111").unwrap()
);
set_test!(value_int8, i8, -1);
set_test!(value_int16, i16, -300);
set_test!(value_int32, i32, -300000);
set_test!(value_int64, i64, -300000);
set_test!(value_int128, i128, -300000);
set_test!(
    value_int256,
    I256,
    I256::from_str("-11111111111111111111111111111111111111111111111111111111").unwrap()
);
set_test!(value_float32, f32, -1.1);
set_test!(value_float64, f64, -1.1);
set_test!(value_bool, bool, true);
set_test!(value_string, String, "test".to_string());
set_test!(
    value_uuid,
    Uuid,
    Uuid::from_str("07f78dad-a68c-4275-8b1e-64101de077ec").unwrap()
);
set_test!(value_date, Date, Date::from_julian_day(1).unwrap());
set_test!(
    value_datetime,
    OffsetDateTime,
    OffsetDateTime::from_unix_timestamp(10).unwrap()
);
set_test!(value_enum8, i8, 0);
set_test!(value_enum16, i16, 300);
set_test!(value_array, Vec<u8>, vec![0, 1, 2]);
set_test!(value_map, HashMap<String, u8>, HashMap::from([("key0".to_string(), 0), ("key1".to_string(), 1)]));
set_test!(value_uint8_null, Option<u8>, None);
set_test!(value_uint16_null, Option<u16>, Some(300));
// ... other tests

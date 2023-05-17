//! Tests

use super::RowBinFormatter;
use crate::value::ChValue;
use assert_hex::assert_eq_hex;
use std::str::FromStr;
use time::{Date, OffsetDateTime};
use uuid::Uuid;

/// Sets a test
macro_rules! set_test {
    // `()` indicates that the macro takes no argument.
    ($ID:ident, $TY:ty, $VAL:expr, $TARGET:expr) => {
        #[test]
        fn $ID() {
            let x: $TY = $VAL;
            let ty = <$TY as ChValue>::ch_type();
            let value = x.into_ch_value();
            let formatter = RowBinFormatter::default();
            let bytes = formatter.format_value(value.clone());
            assert_eq_hex!(bytes, $TARGET);

            let value_parsed = formatter.parse_value(&mut bytes.as_slice(), ty).unwrap();
            assert_eq!(value_parsed, value);
        }
    };
}

set_test!(fmt_rowbin_uint8, u8, 1, 1_u8.to_le_bytes());
set_test!(fmt_rowbin_uint16, u16, 1, 1_u16.to_le_bytes());
set_test!(fmt_rowbin_int8, i8, -1, (-1_i8).to_le_bytes());
set_test!(fmt_rowbin_f64, f64, 1.123_f64, 1.123_f64.to_le_bytes());
set_test!(fmt_rowbin_bool, bool, true, [0x01]);
set_test!(fmt_rowbin_str, String, "ab".to_string(), [0x02, 0x61, 0x62]);
set_test!(
    fmt_rowbin_uuid,
    Uuid,
    Uuid::from_str("866b0a23-7261-4b37-94ed-df3c6365908a").unwrap(),
    {
        let id = Uuid::from_str("866b0a23-7261-4b37-94ed-df3c6365908a").unwrap();
        let (w1, w2) = id.as_u64_pair();
        w1.to_le_bytes()
            .iter()
            .chain(w2.to_le_bytes().iter())
            .copied()
            .collect::<Vec<_>>()
    }
);
set_test!(
    fmt_rowbin_date,
    Date,
    Date::from_ordinal_date(1970, 1).unwrap(),
    0_i32.to_le_bytes()
);
set_test!(
    fmt_rowbin_datetime,
    OffsetDateTime,
    OffsetDateTime::from_unix_timestamp(0).unwrap(),
    0_i64.to_le_bytes()
);

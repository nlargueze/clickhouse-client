//! Extension for the `time` crate

use time::{Date, Month, OffsetDateTime};

use super::Value;

/// Date format
#[cfg(feature = "time")]
pub(crate) static FORMAT_DATE: &[time::format_description::FormatItem] =
    time::macros::format_description!("[year]-[month]-[day]");

/// DateTime format
#[cfg(feature = "time")]
pub(crate) static FORMAT_DATETIME: &[time::format_description::FormatItem] =
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// DateTime format
#[cfg(feature = "time")]
pub(crate) static FORMAT_DATETIME64: &[time::format_description::FormatItem] =
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");

/// Trait to convert a Date to a [Value]
pub trait AsDate32 {
    /// Converts a Date to a Date32
    fn as_date32(&self) -> Value;
}

impl AsDate32 for Date {
    fn as_date32(&self) -> Value {
        let epoch_julian_days = Date::from_calendar_date(1970, Month::January, 1)
            .unwrap()
            .to_julian_day();
        let days = self.to_julian_day() - epoch_julian_days;
        Value::Date32(days)
    }
}

/// Trait to convert a Date to a DateTime64
pub trait AsDateTime64 {
    /// Converts a Date to a DateTime64
    fn as_datetime64(&self) -> Value;
}

impl AsDateTime64 for OffsetDateTime {
    fn as_datetime64(&self) -> Value {
        let dt_ns = self.unix_timestamp_nanos();
        let dt_ns_i64: i64 = dt_ns
            .try_into()
            .expect("datetime is too large to fit in a DT");
        Value::DateTime64(dt_ns_i64)
    }
}

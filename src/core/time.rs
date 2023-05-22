//! Extension for the `time` crate

use time::{Date, Month, OffsetDateTime};

use super::{val::Value, Type, TypeOrm};

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

impl TypeOrm for Date {
    fn db_type() -> super::Type {
        Type::Date32
    }

    fn db_value(&self) -> Value {
        let epoch_julian_days = Date::from_calendar_date(1970, Month::January, 1)
            .unwrap()
            .to_julian_day();
        let days = self.to_julian_day() - epoch_julian_days;
        Value::Date32(days)
    }

    fn from_db_value(_value: &Value) -> Result<Self, crate::orm::prelude::Error>
    where
        Self: Sized,
    {
        todo!();
    }
}

impl TypeOrm for OffsetDateTime {
    fn db_type() -> super::Type {
        Type::DateTime64(9)
    }

    fn db_value(&self) -> Value {
        let dt_ns = self.unix_timestamp_nanos();
        let dt_ns_i64: i64 = dt_ns
            .try_into()
            .expect("datetime is too large to fit in a DT");
        Value::DateTime64(dt_ns_i64)
    }

    fn from_db_value(_value: &Value) -> Result<Self, crate::orm::prelude::Error>
    where
        Self: Sized,
    {
        todo!();
    }
}

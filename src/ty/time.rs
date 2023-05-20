//! Extension for the `time` crate

use serde::Serialize;
use time::{format_description::FormatItem, macros::format_description, Date, OffsetDateTime};

/// Date format
static FORMAT_DATE: &[FormatItem] = format_description!("[year]-[month]-[day]");

/// DateTime format
static _FORMAT_DATETIME: &[FormatItem] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// DateTime format
static FORMAT_DATETIME64: &[FormatItem] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");

/// A Date wrapper for [Date] to implement a custom serializer
///
/// A Clickhouse `Date` has 2 bytes (u16), and `Date32` has 4 bytes (i32).
/// That value is the number of days since 1970-01-01.
///
/// We choose the most general value as Date32
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Date32(pub Date);

impl Serialize for Date32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            let date_str = self.0.format(FORMAT_DATE).expect("invalid date32s");
            serializer.serialize_str(&date_str)
        } else {
            let date_i32 = self.0.to_julian_day()
                - Date::from_calendar_date(1970, time::Month::January, 1)
                    .unwrap()
                    .to_julian_day();
            serializer.serialize_i32(date_i32)
        }
    }
}

/// Trait to convert a Date to a Date32
pub trait AsDate32 {
    /// Converts a Date to a Date32
    fn as_date32(&self) -> Date32;
}

impl AsDate32 for Date {
    fn as_date32(&self) -> Date32 {
        Date32(*self)
    }
}

/// A Date wrapper for [OffsetDateTime] to implement a custom serializer
///
/// We choose the most general value as DateTime64(9) to capture nanoseconds.
///
/// Internally, DteTime64 stores the data as i64, as the number of ‘ticks’ since epoch start.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct DateTime64(pub OffsetDateTime);

impl Serialize for DateTime64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            let date_str = self
                .0
                .format(FORMAT_DATETIME64)
                .expect("invalid datetime64");
            serializer.serialize_str(&date_str)
        } else {
            let date_i64 =
                self.0.unix_timestamp_nanos().try_into().map_err(|_err| {
                    <S::Error as serde::ser::Error>::custom("invalid datetime64")
                })?;
            serializer.serialize_i64(date_i64)
        }
    }
}

/// Trait to convert a Date to a DateTime64
pub trait AsDateTime64 {
    /// Converts a Date to a DateTime64
    fn as_datetime64(&self) -> DateTime64;
}

impl AsDateTime64 for OffsetDateTime {
    fn as_datetime64(&self) -> DateTime64 {
        DateTime64(*self)
    }
}

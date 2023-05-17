//! Extension for `time` crate

use crate::{
    error::Error,
    value::{ChValue, Type, Value},
};

use time::PrimitiveDateTime;
pub use time::{
    format_description::FormatItem, macros::format_description, Date, Month, OffsetDateTime,
};

// -- Date --

/// SQL date format
static FORMAT_YY_MM_DD: &[FormatItem] = format_description!("[year]-[month]-[day]");

/// Extends a [Date] with additional methods
pub trait DateExt: Sized {
    /// Returns the number of days since the beginning of the UNIX period
    fn unix_days(&self) -> i32;

    /// From the number of days since the beginning of the UNIX period, returns the [Date]
    fn from_unix_days(days: i32) -> Result<Self, Error>
    where
        Self: Sized;

    /// Formats as `YYYY-MM-DD`
    fn format_yyyy_mm_dd(&self) -> String;

    /// Parses from `YYYY-MM-DD`
    fn parse_yyyy_mm_dd(value: &str) -> Result<Self, Error>;

    /// Returns the [Date] for the UNIX epoch start
    fn unix_day0() -> Date {
        Date::from_calendar_date(1970, Month::January, 1).unwrap()
    }
}

impl DateExt for Date {
    fn unix_days(&self) -> i32 {
        self.to_julian_day() - Self::unix_day0().to_julian_day()
    }

    fn from_unix_days(days: i32) -> Result<Self, Error> {
        let julian_days = Self::unix_day0().to_julian_day() + days;
        Ok(Date::from_julian_day(julian_days)?)
    }

    fn format_yyyy_mm_dd(&self) -> String {
        self.format(FORMAT_YY_MM_DD).unwrap()
    }

    fn parse_yyyy_mm_dd(value: &str) -> Result<Self, Error> {
        Ok(Date::parse(value, FORMAT_YY_MM_DD)?)
    }
}

// NB: Date is mapped to Value::Date32
impl ChValue for Date {
    fn ch_type() -> crate::value::Type {
        Type::Date32
    }

    fn into_ch_value(self) -> Value {
        Value::Date32(self.unix_days())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::Date(v) => Ok(Date::from_unix_days(v.into())?),
            Value::Date32(v) => Ok(Date::from_unix_days(v)?),
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

// -- PrimitiveDateTime --

/// Extension trait for date times
pub trait DateTimeExt: Sized {
    /// Formats as `YYYY-MM-DD HH:MM:SS`
    fn format_yyyy_mm_dd_hh_mm_ss(&self) -> String;

    /// Formats as `YYYY-MM-DD HH:MM:SS.X`
    fn format_yyyy_mm_dd_hh_mm_ss_ns(&self) -> String;

    /// Parses from `YYYY-MM-DD HH:MM:SS`
    fn parse_yyyy_mm_dd_hh_mm_ss(value: &str) -> Result<Self, Error>;

    /// Parses from `YYYY-MM-DD HH:MM:SS.X`
    fn parse_yyyy_mm_dd_hh_mm_ss_ns(value: &str) -> Result<Self, Error>;

    /// Returns the UNIX seconds
    fn unix_seconds(&self) -> i64;

    /// Returns the UNIX nanoseconds
    fn unix_nanoseconds(&self) -> i64;

    /// Creates from UNIX seconds
    fn from_unix_seconds(value: i64) -> Self;

    /// Creates from UNIX nanoseconds
    fn from_unix_nanoseconds(value: i128) -> Self;
}

/// SQL datetime format
static FORMAT_YY_MM_DD_HH_MM_SS: &[FormatItem] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// SQL datetime format, with subseconds
static FORMAT_YY_MM_DD_HH_MM_SS_NS: &[FormatItem] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");

impl DateTimeExt for PrimitiveDateTime {
    fn format_yyyy_mm_dd_hh_mm_ss(&self) -> String {
        self.format(FORMAT_YY_MM_DD_HH_MM_SS).unwrap()
    }

    fn format_yyyy_mm_dd_hh_mm_ss_ns(&self) -> String {
        self.format(FORMAT_YY_MM_DD_HH_MM_SS_NS).unwrap()
    }

    fn parse_yyyy_mm_dd_hh_mm_ss(value: &str) -> Result<Self, Error> {
        Ok(PrimitiveDateTime::parse(value, FORMAT_YY_MM_DD_HH_MM_SS)?)
    }

    fn parse_yyyy_mm_dd_hh_mm_ss_ns(value: &str) -> Result<Self, Error> {
        Ok(PrimitiveDateTime::parse(
            value,
            FORMAT_YY_MM_DD_HH_MM_SS_NS,
        )?)
    }

    fn unix_seconds(&self) -> i64 {
        self.assume_utc().unix_seconds()
    }

    fn unix_nanoseconds(&self) -> i64 {
        self.assume_utc().unix_nanoseconds()
    }

    fn from_unix_seconds(value: i64) -> Self {
        let dt = OffsetDateTime::from_unix_seconds(value);
        let date = dt.date();
        let time = dt.time();
        PrimitiveDateTime::new(date, time)
    }

    fn from_unix_nanoseconds(value: i128) -> Self {
        let dt = OffsetDateTime::from_unix_nanoseconds(value);
        let date = dt.date();
        let time = dt.time();
        PrimitiveDateTime::new(date, time)
    }
}

// NB: OffsetDateTime is mapped to Value::DateTime64
impl ChValue for PrimitiveDateTime {
    fn ch_type() -> Type {
        Type::DateTime64(9)
    }

    fn into_ch_value(self) -> Value {
        Value::DateTime64(self.unix_nanoseconds())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::DateTime(secs) => Ok(Self::from_unix_seconds(secs.into())),
            Value::DateTime64(nanosecs) => Ok(Self::from_unix_nanoseconds(nanosecs.into())),
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

// -- OffsetDateTime --

impl DateTimeExt for OffsetDateTime {
    fn format_yyyy_mm_dd_hh_mm_ss(&self) -> String {
        self.format(FORMAT_YY_MM_DD_HH_MM_SS).unwrap()
    }

    fn format_yyyy_mm_dd_hh_mm_ss_ns(&self) -> String {
        self.format(FORMAT_YY_MM_DD_HH_MM_SS_NS).unwrap()
    }

    fn parse_yyyy_mm_dd_hh_mm_ss(value: &str) -> Result<Self, Error> {
        Ok(PrimitiveDateTime::parse_yyyy_mm_dd_hh_mm_ss(value)?.assume_utc())
    }

    fn parse_yyyy_mm_dd_hh_mm_ss_ns(value: &str) -> Result<Self, Error> {
        Ok(PrimitiveDateTime::parse_yyyy_mm_dd_hh_mm_ss_ns(value)?.assume_utc())
    }

    fn unix_seconds(&self) -> i64 {
        self.unix_timestamp()
    }

    fn unix_nanoseconds(&self) -> i64 {
        let ns = self.unix_timestamp_nanos();
        if ns <= i64::MIN as i128 {
            i64::MIN
        } else if ns >= i64::MAX as i128 {
            i64::MAX
        } else {
            ns as i64
        }
    }

    fn from_unix_seconds(value: i64) -> Self {
        Self::from_unix_timestamp(value).expect("invalid unix timestamp")
    }

    fn from_unix_nanoseconds(value: i128) -> Self {
        Self::from_unix_timestamp_nanos(value).expect("invalid unix timestamp")
    }
}

// NB: OffsetDateTime is mapped to Value::DateTime64
impl ChValue for OffsetDateTime {
    fn ch_type() -> Type {
        Type::DateTime64(9)
    }

    fn into_ch_value(self) -> Value {
        Value::DateTime64(self.unix_nanoseconds())
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::DateTime(secs) => Ok(Self::from_unix_seconds(secs.into())),
            Value::DateTime64(nanosecs) => Ok(Self::from_unix_nanoseconds(nanosecs.into())),
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

// -- Option<Date> --

impl ChValue for Option<Date> {
    fn ch_type() -> Type {
        Type::NullableDate32
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => Value::NullableDate32(Some(v.unix_days())),
            None => Value::NullableDate32(None),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableDate(v) => match v {
                Some(v) => Date::from_unix_days(v.into()).map(Some),
                None => Ok(None),
            },
            Value::NullableDate32(v) => match v {
                Some(v) => Date::from_unix_days(v).map(Some),
                None => Ok(None),
            },
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

// -- Option<PrimitiveDateTime> --

impl ChValue for Option<PrimitiveDateTime> {
    fn ch_type() -> Type {
        Type::NullableDateTime64(9)
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => PrimitiveDateTime::into_ch_value(v),
            None => Value::NullableDateTime64(None),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableDateTime(secs) => match secs {
                Some(secs) => Ok(Some(PrimitiveDateTime::from_unix_seconds(secs.into()))),
                None => Ok(None),
            },
            Value::NullableDateTime64(nanosecs) => match nanosecs {
                Some(nanosecs) => Ok(Some(PrimitiveDateTime::from_unix_nanoseconds(
                    nanosecs.into(),
                ))),
                None => Ok(None),
            },
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

// -- Option<OffsetDateTime> --

impl ChValue for Option<OffsetDateTime> {
    fn ch_type() -> Type {
        Type::NullableDateTime64(9)
    }

    fn into_ch_value(self) -> Value {
        match self {
            Some(v) => OffsetDateTime::into_ch_value(v),
            None => Value::NullableDateTime64(None),
        }
    }

    fn from_ch_value(value: Value) -> Result<Self, Error> {
        match value {
            Value::NullableDateTime(secs) => match secs {
                Some(secs) => Ok(Some(OffsetDateTime::from_unix_seconds(secs.into()))),
                None => Ok(None),
            },
            Value::NullableDateTime64(nanosecs) => match nanosecs {
                Some(nanosecs) => Ok(Some(OffsetDateTime::from_unix_nanoseconds(nanosecs.into()))),
                None => Ok(None),
            },
            _ => Err(Error::new("Cannot convert Value to base type")),
        }
    }
}

//! Data types
//!
//! This modules provides the Clickhouse data types.
//!
//! Types are defined at [https://clickhouse.com/docs/en/sql-reference/data-types](https://clickhouse.com/docs/en/sql-reference/data-types).

use std::fmt::Debug;

/// Trait for Clickhouse types
pub trait DbType: Debug {
    /// Converts a type to a SQL string
    fn to_sql_str(&self) -> String;

    /// Parses from a SQL string
    fn from_sql_str(s: &str) -> Result<Self, String>
    where
        Self: Sized;
}

/// Implements a basic type which implements `to_string()`
macro_rules! impl_basic_type {
    ( $( $TY:ty ),* ) => {
        $(
            impl DbType for $TY {
                fn to_sql_str(&self) -> String {
                    self.to_string()
                }

                fn from_sql_str(s: &str) -> Result<Self, String> {
                    s.parse::<$TY>().map_err(|e| e.to_string())
                }
            }
        )*
    };
}

impl_basic_type!(u8, u16, u32, u64, u128);
impl_basic_type!(i8, i16, i32, i64, i128);
impl_basic_type!(f32, f64);
impl_basic_type!(bool);

// NB: in SQL, strings muts be single quoted
impl DbType for String {
    fn to_sql_str(&self) -> String {
        format!("'{}'", self)
    }

    fn from_sql_str(s: &str) -> Result<Self, String> {
        s.parse::<String>().map_err(|e| e.to_string())
    }
}

/// Implements the nullable variant
impl<T> DbType for Option<T>
where
    T: DbType,
{
    fn to_sql_str(&self) -> String {
        match self {
            Some(value) => value.to_sql_str(),
            None => "NULL".to_string(),
        }
    }

    fn from_sql_str(s: &str) -> Result<Self, String> {
        match s {
            // NB: it seems to be the returned NULL value in Clickhouse
            r"\N" => Ok(None),
            _ => T::from_sql_str(s).map(|x| Some(x)),
        }
    }
}

#[cfg(feature = "time")]
mod time {
    //! Data types extension for the `time` crate
    use ::time::{macros::format_description, OffsetDateTime, PrimitiveDateTime, UtcOffset};

    use super::*;

    impl DbType for OffsetDateTime {
        fn to_sql_str(&self) -> String {
            // NB: CLickhouse accept the format
            // - '2023-05-06 20:03:15'
            // - Unix timestamp
            //
            // NB2: CLickhouse accepts only UTC dates (the timezone is a column metadata)
            let date_utc = self.to_offset(UtcOffset::UTC);
            let format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
            format!("'{}'", date_utc.format(format).unwrap())
        }

        fn from_sql_str(s: &str) -> Result<Self, String> {
            let format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
            let prim_dt = PrimitiveDateTime::parse(s, &format).map_err(|e| e.to_string())?;
            Ok(prim_dt.assume_offset(UtcOffset::UTC))
        }
    }
}

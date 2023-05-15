//! Data types
//!
//! This modules provides the Clickhouse data types.
//!
//! Types are defined at [https://clickhouse.com/docs/en/sql-reference/data-types](https://clickhouse.com/docs/en/sql-reference/data-types).

use std::{collections::HashMap, fmt::Debug};

/// Trait to implement a DB type
pub trait DbType: Debug {
    /// Clickhouse cell type
    const TYPE: &'static str;
}

/// Trait to convert a db value from and into a string
pub trait DbValue: Debug {
    /// Converts a type to a SQL string
    fn to_sql_str(&self) -> String;

    /// Parses from a SQL string
    fn from_sql_str(s: &str) -> Result<Self, String>
    where
        Self: Sized;
}

/// Implements the nullable variant of a db type
macro_rules! impl_db_nullable_variant {
    ($TY:ty, $DB_TY:tt) => {
        impl DbType for Option<$TY> {
            const TYPE: &'static str = concat!("Nullable(", $DB_TY, ")");
        }

        impl DbValue for Option<$TY> {
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
                    _ => <$TY as DbValue>::from_sql_str(s).map(|x| Some(x)),
                }
            }
        }
    };
}

/// Implements the array variant of a db type
macro_rules! impl_db_array_variant {
    ($TY:ty, $DB_TY:tt) => {
        impl DbType for Vec<$TY> {
            const TYPE: &'static str = concat!("Array(", $DB_TY, ")");
        }

        impl DbValue for Vec<$TY>
        where
            $TY: DbValue,
        {
            fn to_sql_str(&self) -> String {
                format!(
                    "[{}]",
                    self.iter()
                        .map(|x| x.to_sql_str())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }

            fn from_sql_str(s: &str) -> Result<Self, String> {
                let s = match s.strip_prefix('[') {
                    Some(s) => s,
                    None => return Err("Invalid array".to_string()),
                };
                let s = match s.strip_suffix(']') {
                    Some(s) => s,
                    None => return Err("Invalid array".to_string()),
                };
                s.split(",")
                    .into_iter()
                    .map(|v_str| <$TY as DbValue>::from_sql_str(v_str))
                    .collect::<Result<Vec<_>, String>>()
            }
        }
    };
}

/// Implements the map variant of a db type
macro_rules! impl_db_map_variant {
    ($TY:ty, $DB_TY:tt) => {
        impl DbType for HashMap<String, $TY> {
            const TYPE: &'static str = concat!("Map(String, ", $DB_TY, ")");
        }

        impl DbValue for HashMap<String, $TY>
        where
            $TY: DbValue,
        {
            fn to_sql_str(&self) -> String {
                // {'key1':1, 'key2':10}
                format!(
                    "{{{}}}",
                    self.iter()
                        .map(|(k, v)| format!("'{}':{}", k, v.to_sql_str()))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }

            fn from_sql_str(s: &str) -> Result<Self, String> {
                let s = match s.strip_prefix('{') {
                    Some(s) => s,
                    None => return Err("Invalid map".to_string()),
                };
                let s = match s.strip_suffix('}') {
                    Some(s) => s,
                    None => return Err("Invalid map".to_string()),
                };
                s.split(",")
                    .into_iter()
                    .map(|part| {
                        // item is 'key1':1
                        match part.split_once(':') {
                            Some((k, v)) => {
                                let v = match <$TY as DbValue>::from_sql_str(v) {
                                    Ok(v) => v,
                                    Err(err) => return Err(err),
                                };
                                Ok((k.to_string(), v))
                            }
                            None => Err("Invalid map".to_string()),
                        }
                    })
                    .collect::<Result<HashMap<String, _>, String>>()
            }
        }
    };
}

/// Implements a base type which implements `to_string()` and `FromStr`
macro_rules! impl_db_base_type {
    ($TY:ty, $DB_TY:tt) => {
        impl DbType for $TY {
            const TYPE: &'static str = $DB_TY;
        }

        impl DbValue for $TY {
            fn to_sql_str(&self) -> String {
                self.to_string()
            }

            fn from_sql_str(s: &str) -> Result<Self, String> {
                s.parse::<$TY>().map_err(|e| e.to_string())
            }
        }

        impl_db_nullable_variant!($TY, $DB_TY);
        impl_db_array_variant!($TY, $DB_TY);
        impl_db_map_variant!($TY, $DB_TY);
    };
}

// base types
impl_db_base_type!(u8, "UInt8");
impl_db_base_type!(u16, "UInt16");
impl_db_base_type!(u32, "UInt32");
impl_db_base_type!(u64, "UInt64");
impl_db_base_type!(u128, "UInt128");
impl_db_base_type!(i8, "Int8");
impl_db_base_type!(i16, "Int16");
impl_db_base_type!(i32, "Int32");
impl_db_base_type!(i64, "Int64");
impl_db_base_type!(i128, "Int128");
impl_db_base_type!(f32, "Float32");
impl_db_base_type!(f64, "Float64");
impl_db_base_type!(bool, "Boolean");

// string
impl DbType for String {
    const TYPE: &'static str = "String";
}

impl DbValue for String {
    fn to_sql_str(&self) -> String {
        // NB: strings must be enclosed by '
        format!("'{}'", self)
    }

    fn from_sql_str(s: &str) -> Result<Self, String> {
        s.parse::<String>().map_err(|e| e.to_string())
    }
}

impl_db_nullable_variant!(String, "String");
impl_db_array_variant!(String, "String");
impl_db_map_variant!(String, "String");

// enum
/// Extension for the `time` crate
#[cfg(feature = "time")]
mod time {
    use super::*;

    use ::time::{macros::format_description, OffsetDateTime, PrimitiveDateTime, UtcOffset};

    impl DbType for OffsetDateTime {
        const TYPE: &'static str = "DateTime64(9)";
    }

    impl DbValue for OffsetDateTime {
        fn to_sql_str(&self) -> String {
            // NB: CLickhouse accept the format
            // - '2023-05-06 20:03:15'
            // - Unix timestamp
            //
            // NB2: Clickhouse accepts only UTC dates (the timezone is a column metadata)
            let date_utc = self.to_offset(UtcOffset::UTC);
            let format =
                format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
            format!("'{}'", date_utc.format(format).unwrap())
        }

        fn from_sql_str(s: &str) -> Result<Self, String> {
            let format =
                format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
            let prim_dt = PrimitiveDateTime::parse(s, &format).map_err(|e| e.to_string())?;
            Ok(prim_dt.assume_offset(UtcOffset::UTC))
        }
    }

    impl_db_nullable_variant!(OffsetDateTime, "DateTime64");
}

/// Extension for the `uuid` crate
#[cfg(feature = "uuid")]
mod uuid {
    use super::*;

    use ::uuid::Uuid;

    impl DbType for Uuid {
        const TYPE: &'static str = "UUID";
    }

    impl DbValue for Uuid {
        fn to_sql_str(&self) -> String {
            self.to_string()
        }

        fn from_sql_str(s: &str) -> Result<Self, String> {
            s.parse::<Uuid>().map_err(|e| e.to_string())
        }
    }
}

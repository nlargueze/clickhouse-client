//! Data types
//!
//! This modules provides the Clickhouse data types.
//!
//! Types are defined at [https://clickhouse.com/docs/en/sql-reference/data-types](https://clickhouse.com/docs/en/sql-reference/data-types).

use std::collections::HashMap;

use super::DbType;

/// Implements the nullable variant of a db type
macro_rules! impl_db_nullable_variant {
    ($TY:ty, $DB_TY:tt) => {
        impl DbType for Option<$TY> {
            const TYPE: &'static str = concat!("Nullable(", $DB_TY, ")");
        }

        impl DbType for Option<$TY> {
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
                    _ => <$TY as DbType>::from_sql_str(s).map(|x| Some(x)),
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

        impl DbType for Vec<$TY>
        where
            $TY: DbType,
        {
            fn to_sql_str(&self) -> String {
                format!(
                    "[{}]",
                    self.iter()
                        .map(|x| x.to_sql_str())
                        .collect::<Vec<_>>()
                        .join(", ")
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
                    .map(|v_str| <$TY as DbType>::from_sql_str(v_str))
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

        impl DbType for HashMap<String, $TY>
        where
            $TY: DbType,
        {
            fn to_sql_str(&self) -> String {
                // {'key1':1, 'key2':10}
                format!(
                    "{{{}}}",
                    self.iter()
                        .map(|(k, v)| format!("{}:{}", k.to_sql_str(), v.to_sql_str()))
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
                                let k = match String::from_sql_str(k) {
                                    Ok(k) => k,
                                    Err(err) => return Err(err),
                                };

                                let v = match <$TY as DbValue>::from_sql_str(v) {
                                    Ok(v) => v,
                                    Err(err) => return Err(err),
                                };
                                Ok((k, v))
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

        impl DbType for $TY {
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

impl_db_nullable_variant!(String, "String");
impl_db_array_variant!(String, "String");
impl_db_map_variant!(String, "String");

//! Data types
//!
//! This modules provides the Clickhouse data types.
//!
//! Types are defined at [https://clickhouse.com/docs/en/sql-reference/data-types](https://clickhouse.com/docs/en/sql-reference/data-types).

use std::fmt::Debug;

/// Trait for Clickhouse types
pub trait DbType: Debug {}

impl DbType for u8 {}
impl DbType for u16 {}
impl DbType for u32 {}
impl DbType for u64 {}
impl DbType for u128 {}
impl DbType for String {}

// Implement DbType for all references to types which implement DbType
impl<'a, T> DbType for &'a T where T: DbType {}
impl<'a, T> DbType for &'a mut T where T: DbType {}

#[cfg(feature = "time")]
mod time {
    use super::*;

    impl DbType for ::time::OffsetDateTime {}
}
// impl_basic_type!(UInt8, u8, "UInt8 type");
// impl_basic_type!(UInt16, u16, "UInt16 type");

// // /// Basic data types
// // #[derive(Debug, PartialEq)]
// // pub enum DataType {
// //     /// null
// //     Null,
// //     /// u8
// //     UInt8(u8),
// //     /// u16
// //     UInt16(u16),
// //     /// u32
// //     UInt32(u32),
// //     /// u64
// //     UInt64(u64),
// //     /// u128
// //     UInt128(u128),
// //     /// u256
// //     UInt256(u128),
// //     /// i8 (alias TINYINT, BOOL, BOOLEAN, INT1)
// //     Int8(i8),
// //     /// i16 (alias SMALLINT, INT2)
// //     Int16(i16),
// //     /// i32 (alias INT, INT4, INTEGER)
// //     Int32(i32),
// //     /// i64 (alias BIGINT)
// //     Int64(i64),
// //     /// i128
// //     Int128(i128),
// //     /// i256
// //     Int256(i128),
// //     /// float32
// //     Float32(f32),
// //     /// float64
// //     Float64(f64),
// //     /// boolean (true (1), false (0))
// //     Boolean(bool),
// //     /// String (alias LONGTEXT, MEDIUMTEXT, TINYTEXT, TEXT, LONGBLOB, MEDIUMBLOB, TINYBLOB, BLOB, VARCHAR, CHAR)
// //     ///
// //     /// There is no encoding, just bytes
// //     String(String),
// //     /// Fixed length string (FixedString(N))
// //     FixedString(String),
// //     /// Date (format '1970-01-01', or epoch in secs, stored as 2 bytes)
// //     #[cfg(feature = "time")]
// //     Date(time::Date),
// //     /// Date (stored as i32, format '1970-01-01', or epoch in secs)
// //     #[cfg(feature = "time")]
// //     Date32(time::Date),
// //     /// Date time (format '1970-01-01 00:00:00', or unix timestamp, timezone stored in the col metadata)
// //     #[cfg(feature = "time")]
// //     DateTime(time::OffsetDateTime),
// //     /// Date time with subseconds precision (format '2299-12-31 23:59:59.99999999]')
// //     #[cfg(feature = "time")]
// //     DateTime64(time::OffsetDateTime),
// //     /// JSON (experimental)
// //     Json(Vec<u8>),
// //     /// UUID (16-byte number, uses UUID V4)
// //     Uuid(String),
// //     //Enum (enum with a few values, eg 'Enum('hello' = 1, 'world' = 2)')
// //     Enum,
// //     /// LowCardinality(data_type), small dictionnary
// //     LowCardinality,
// //     /// Array
// //     ///
// //     /// ```sql
// //     /// CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();
// //     /// INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);
// //     /// SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
// //     /// ```
// //     Array,
// //     /// Dictionnary
// //     ///
// //     /// ```sql
// //     /// CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
// //     /// INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
// //     /// ```
// //     Map,
// //     /// Nested table inside a cell
// //     Nested,
// //     /// Tuple of heterogenous types
// //     Tuple,
// //     /// u32
// //     IPv4(u32),
// //     /// Fixed string (16)
// //     IPv6(String),
// //     /// Point
// //     Point(f64, f64),
// //     /// Ring (array of Point)
// //     Ring,
// //     /// Array(Ring)
// //     Polygon,
// //     /// Array(Polygon)
// //     MultiPolygon,
// // }

//! Data types

#[cfg(feature = "time")]
pub mod time;

#[cfg(test)]
pub mod tests;

use std::{collections::HashMap, str::FromStr};

use once_cell::sync::OnceCell;
use regex::Regex;

use crate::error::Error;

/// Data type in the database
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Type {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    /// Decimal (precision ∈ [1:76], scale ∈ [0:P])
    Decimal(u8, u8),
    /// Decimal with scale
    Decimal32(u8),
    Decimal64(u8),
    Decimal128(u8),
    Decimal256(u8),
    Bool,
    String,
    FixedString(u64),
    Date,
    Date32,
    DateTime,
    DateTime64(u8),
    UUID,
    /// Array
    ///
    /// An array element can have any type
    Array(Box<Type>),
    /// Map
    ///
    /// - key: String, Integer, LowCardinality, FixedString, UUID, Date, DateTime, Date32, Enum
    /// - value: any type
    Map(Box<Type>, Box<Type>),
    /// Nested
    ///
    /// A nested structure is a table inside a cell.
    ///
    /// Each type can have a name (optional)
    Nested(HashMap<Option<String>, Box<Type>>),
    // TODO: tuple
    NullableUInt8,
    NullableUInt16,
    NullableUInt32,
    NullableUInt64,
    NullableUInt128,
    NullableUInt256,
    NullableInt8,
    NullableInt16,
    NullableInt32,
    NullableInt64,
    NullableInt128,
    NullableInt256,
    NullableFloat32,
    NullableFloat64,
    NullableDecimal(u8, u8),
    NullableDecimal32(u8),
    NullableDecimal64(u8),
    NullableDecimal128(u8),
    NullableDecimal256(u8),
    NullableBool,
    NullableString,
    NullableFixedString(u64),
    NullableDate,
    NullableDate32,
    NullableDateTime,
    NullableDateTime64(u8),
    NullableUUID,
}

impl Type {
    /// Converts a type ots its nullable variant
    pub fn to_nullable(&self) -> Result<Type, Error> {
        match self {
            Type::UInt8 => Ok(Type::NullableUInt8),
            Type::UInt16 => Ok(Type::NullableUInt16),
            Type::UInt32 => Ok(Type::NullableUInt32),
            Type::UInt64 => Ok(Type::NullableUInt64),
            Type::UInt128 => Ok(Type::NullableUInt128),
            Type::UInt256 => Ok(Type::NullableUInt256),
            Type::Int8 => Ok(Type::NullableInt8),
            Type::Int16 => Ok(Type::NullableInt16),
            Type::Int32 => Ok(Type::NullableInt32),
            Type::Int64 => Ok(Type::NullableInt64),
            Type::Int128 => Ok(Type::NullableInt128),
            Type::Int256 => Ok(Type::NullableInt256),
            Type::Float32 => Ok(Type::NullableFloat32),
            Type::Float64 => Ok(Type::NullableFloat64),
            Type::Decimal(p, s) => Ok(Type::Decimal(*p, *s)),
            Type::Decimal32(s) => Ok(Type::Decimal32(*s)),
            Type::Decimal64(s) => Ok(Type::Decimal64(*s)),
            Type::Decimal128(s) => Ok(Type::Decimal128(*s)),
            Type::Decimal256(s) => Ok(Type::Decimal256(*s)),
            Type::Bool => Ok(Type::NullableBool),
            Type::String => Ok(Type::NullableString),
            Type::FixedString(n) => Ok(Type::NullableFixedString(*n)),
            Type::Date => Ok(Type::NullableDate),
            Type::Date32 => Ok(Type::NullableDate32),
            Type::DateTime => Ok(Type::NullableDateTime),
            Type::DateTime64(p) => Ok(Type::NullableDateTime64(*p)),
            Type::UUID => Ok(Type::NullableUUID),
            _ => Err(Error(format!("type {self} is not nullable"))),
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = match self {
            Type::UInt8 => "UInt8".into(),
            Type::UInt16 => "UInt16".into(),
            Type::UInt32 => "UInt32".into(),
            Type::UInt64 => "UInt64".into(),
            Type::UInt128 => "UInt128".into(),
            Type::UInt256 => "UInt256".into(),
            Type::Int8 => "Int8".into(),
            Type::Int16 => "Int16".into(),
            Type::Int32 => "Int32".into(),
            Type::Int64 => "Int64".into(),
            Type::Int128 => "Int128".into(),
            Type::Int256 => "Int256".into(),
            Type::Float32 => "Float32".into(),
            Type::Float64 => "Float64".into(),
            Type::Decimal(p, s) => format!("Decimal({p},{s})"),
            Type::Decimal32(s) => format!("Decimal32({s})"),
            Type::Decimal64(s) => format!("Decimal64({s})"),
            Type::Decimal128(s) => format!("Decimal128({s})"),
            Type::Decimal256(s) => format!("Decimal256({s})"),
            Type::Bool => "Bool".into(),
            Type::String => "String".into(),
            Type::FixedString(n) => format!("FixedString({n})"),
            Type::Date => "Date".into(),
            Type::Date32 => "Date32".into(),
            Type::DateTime => "DateTime".into(),
            Type::DateTime64(p) => format!("DateTime64({p})"),
            Type::UUID => "UUID".into(),
            Type::Array(t) => format!("Array({t})"),
            Type::Map(k, v) => format!("Map({k},{v})"),
            Type::Nested(map) => {
                format!(
                    "Nested({})",
                    map.iter()
                        .map(|(name, ty)| {
                            //
                            format!(
                                "{}{}",
                                name.clone().map(|s| format!("{s} ")).unwrap_or_default(),
                                ty
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            // nullable
            Type::NullableUInt8 => "Nullable(UInt8)".into(),
            Type::NullableUInt16 => "Nullable(UInt16)".into(),
            Type::NullableUInt32 => "Nullable(UInt32)".into(),
            Type::NullableUInt64 => "Nullable(UInt64)".into(),
            Type::NullableUInt128 => "Nullable(UInt128)".into(),
            Type::NullableUInt256 => "Nullable(UInt256)".into(),
            Type::NullableInt8 => "Nullable(Int8)".into(),
            Type::NullableInt16 => "Nullable(Int16)".into(),
            Type::NullableInt32 => "Nullable(Int32)".into(),
            Type::NullableInt64 => "Nullable(Int64)".into(),
            Type::NullableInt128 => "Nullable(Int128)".into(),
            Type::NullableInt256 => "Nullable(Int256)".into(),
            Type::NullableFloat32 => "Nullable(Float32)".into(),
            Type::NullableFloat64 => "Nullable(Float64)".into(),
            Type::NullableDecimal(p, s) => format!("Nullable(Decimal({p},{s}))"),
            Type::NullableDecimal32(s) => format!("Nullable(Decimal32({s}))"),
            Type::NullableDecimal64(s) => format!("Nullable(Decimal64({s}))"),
            Type::NullableDecimal128(s) => format!("Nullable(Decimal128({s}))"),
            Type::NullableDecimal256(s) => format!("Nullable(Decimal256({s}))"),
            Type::NullableBool => "Nullable(Bool)".into(),
            Type::NullableString => "Nullable(String)".into(),
            Type::NullableFixedString(n) => format!("Nullable(FixedString({n}))"),
            Type::NullableDate => "Nullable(Date)".into(),
            Type::NullableDate32 => "Nullable(Date32)".into(),
            Type::NullableDateTime => "Nullable(DateTime)".into(),
            Type::NullableDateTime64(p) => format!("Nullable(DateTime64({p}))"),
            Type::NullableUUID => "Nullable(UUID)".into(),
        };
        write!(f, "{s}")
    }
}

impl FromStr for Type {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        /// A regex for nullable types
        static REGEX_NULLABLE: OnceCell<Regex> = OnceCell::new();

        /// A regex for DateTime64
        static REGEX_DATETIME64: OnceCell<Regex> = OnceCell::new();

        /// A regex for Decimal256(s)
        static REGEX_DECIMAL256: OnceCell<Regex> = OnceCell::new();

        /// A regex for Decimal128(s)
        static REGEX_DECIMAL128: OnceCell<Regex> = OnceCell::new();

        /// A regex for Decimal64(s)
        static REGEX_DECIMAL64: OnceCell<Regex> = OnceCell::new();

        /// A regex for Decimal32(s)
        static REGEX_DECIMAL32: OnceCell<Regex> = OnceCell::new();

        /// A regex for Decimal(s)
        static REGEX_DECIMAL: OnceCell<Regex> = OnceCell::new();

        /// A regex for FixedString(n)
        static REGEX_FIXEDSTRING: OnceCell<Regex> = OnceCell::new();

        // nullable
        let regex_nullable =
            REGEX_NULLABLE.get_or_init(|| Regex::new(r"Nullable\((?P<ty>[[:word:]]+)\)").unwrap());
        if let Some(caps) = regex_nullable.captures(s) {
            if let Some(m) = caps.name("ty") {
                let inner_str = m.as_str();
                let inner_ty = inner_str.parse::<Type>()?;
                return inner_ty.to_nullable();
            }
        }

        match s {
            "UInt8" => Ok(Type::UInt8),
            "UInt16" => Ok(Type::UInt16),
            "UInt32" => Ok(Type::UInt32),
            "UInt64" => Ok(Type::UInt64),
            "UInt128" => Ok(Type::UInt128),
            "UInt256" => Ok(Type::UInt256),
            "Int8" => Ok(Type::Int8),
            "Int16" => Ok(Type::Int16),
            "Int32" => Ok(Type::Int32),
            "Int64" => Ok(Type::Int64),
            "Int128" => Ok(Type::Int128),
            "Int256" => Ok(Type::Int256),
            "Float32" => Ok(Type::Float32),
            "Float64" => Ok(Type::Float64),
            "Bool" => Ok(Type::Bool),
            "String" => Ok(Type::String),
            "Date" => Ok(Type::Date),
            "Date32" => Ok(Type::Date32),
            "DateTime" => Ok(Type::DateTime),
            "UUID" => Ok(Type::UUID),
            _ => {
                // Decinal
                //     Type::Decimal(p, s) => format!("Decimal({p},{s})"),
                // Type::Decimal32(s) => format!("Decimal32({s})"),
                // Type::Decimal64(s) => format!("Decimal64({s})"),
                // Type::Decimal128(s) => format!("Decimal128({s})"),
                // Type::Decimal256(s) => format!("Decimal256({s})"),

                // DateTime64
                if s.starts_with("DateTime64") {
                    let regex_datetime64 = REGEX_DATETIME64
                        .get_or_init(|| Regex::new(r"DateTime64\((?P<precision>\d{1})\)").unwrap());
                    if let Some(caps) = regex_datetime64.captures(s) {
                        if let Some(m) = caps.name("precision") {
                            if m.end() < s.len() {
                                return Err(Error(format!("invalid DateTime64 type: {s}")));
                            }
                            let i: u8 = m.as_str().parse()?;
                            return Ok(Type::DateTime64(i));
                        }
                    } else {
                        return Err(Error(format!("invalid DateTime64 type: {s}")));
                    }
                }

                // // FixedString
                // if s.starts_with("FixedString") {
                //     let regex_fixestring = REGEX_FIXEDSTRING
                //         .get_or_init(|| Regex::new(r"FixedString\(\d{1}\)").unwrap());
                //     if let Some(m) = regex_fixestring.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid FixedString type: {s}")));
                //         }
                //         let n: u64 = m.as_str().parse()?;
                //         return Ok(Type::FixedString(n));
                //     } else {
                //         return Err(Error(format!("invalid FixedString type: {s}")));
                //     }
                // }

                // // Decimal256
                // if s.starts_with("Decimal256") {
                //     let regex_dec256 = REGEX_DECIMAL256
                //         .get_or_init(|| Regex::new(r"Decimal256\(\d{1}\)").unwrap());
                //     if let Some(m) = regex_dec256.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid Decimal256 type: {s}")));
                //         }
                //         let i: u8 = m.as_str().parse()?;
                //         return Ok(Type::Decimal256(i));
                //     } else {
                //         return Err(Error(format!("invalid Decimal256 type: {s}")));
                //     }
                // }

                // // Decimal128
                // if s.starts_with("Decimal128") {
                //     let regex_dec128 = REGEX_DECIMAL128
                //         .get_or_init(|| Regex::new(r"Decimal128\(\d{1}\)").unwrap());
                //     if let Some(m) = regex_dec128.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid Decimal128 type: {s}")));
                //         }
                //         let i: u8 = m.as_str().parse()?;
                //         return Ok(Type::Decimal128(i));
                //     } else {
                //         return Err(Error(format!("invalid Decimal128 type: {s}")));
                //     }
                // }

                // // Decimal64
                // if s.starts_with("Decimal64") {
                //     let regex_dec64 =
                //         REGEX_DECIMAL64.get_or_init(|| Regex::new(r"Decimal64\(\d{1}\)").unwrap());
                //     if let Some(m) = regex_dec64.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid Decimal64 type: {s}")));
                //         }
                //         let i: u8 = m.as_str().parse()?;
                //         return Ok(Type::Decimal64(i));
                //     } else {
                //         return Err(Error(format!("invalid Decimal64 type: {s}")));
                //     }
                // }

                // // Decimal32
                // if s.starts_with("Decimal32") {
                //     let regex_dec32 =
                //         REGEX_DECIMAL32.get_or_init(|| Regex::new(r"Decimal32\(\d{1}\)").unwrap());
                //     if let Some(m) = regex_dec32.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid Decimal32 type: {s}")));
                //         }
                //         let i: u8 = m.as_str().parse()?;
                //         return Ok(Type::Decimal32(i));
                //     } else {
                //         return Err(Error(format!("invalid Decimal32 type: {s}")));
                //     }
                // }

                // // Decimal
                // if s.starts_with("Decimal") {
                //     let regex_dec = REGEX_DECIMAL
                //         .get_or_init(|| Regex::new(r"Decimal\(\d{1},\d{1}\)").unwrap());
                //     if let Some(m) = regex_dec.find(s) {
                //         if m.end() < s.len() {
                //             return Err(Error(format!("invalid Decimal type: {s}")));
                //         }
                //         let i: u8 = m.as_str().parse()?;
                //         return Ok(Type::Decimal32(i));
                //     } else {
                //         return Err(Error(format!("invalid Decimal type: {s}")));
                //     }
                // }

                Err(Error(format!("invalid db type: {s}")))
            }
        }
    }
}

// /// Defines a DB type
// macro_rules! define_type {
//     ($DB_TY:ident, $TY:ty, $DOC:tt) => {
//         #[doc = $DOC]
//         #[derive(Debug)]
//         pub struct $DB_TY(pub $TY);

//         impl ::std::ops::Deref for $DB_TY {
//             type Target = $TY;

//             fn deref(&self) -> &Self::Target {
//                 &self.0
//             }
//         }

//         impl ::std::ops::DerefMut for $DB_TY {
//             fn deref_mut(&mut self) -> &mut Self::Target {
//                 &mut self.0
//             }
//         }
//     };
// }

// define_type!(UInt8, u8, "UInt8 type");
// define_type!(UInt16, u16, "UInt16 type");
// define_type!(UInt32, u32, "UInt32 type");
// define_type!(UInt64, u64, "UInt64 type");
// define_type!(UInt128, u128, "UInt128 type");
// define_type!(Int8, i8, "Int8 type");
// define_type!(Int16, i16, "Int16 type");
// define_type!(Int32, i32, "Int32 type");
// define_type!(Int64, i64, "Int64 type");
// define_type!(Int128, i128, "Int128 type");
// define_type!(Float32, f32, "Float32 type");
// define_type!(Float64, f32, "Float64 type");
// define_type!(Bool, bool, "Boolean type");
// // define_type!(String, String, "String type");
// define_type!(Uuid, [u8; 16], "UUID type");
// define_type!(Date, u16, "Date type (days since 1970-01-01)");
// define_type!(Date32, i32, "Date32 type (days since/before 1970-01-01)");
// define_type!(DateTime, u32, "DateTime type");
// define_type!(DateTime64, u64, "DateTime64 type");

// /// Clickhouse data type
// #[derive(Debug)]
// #[allow(missing_docs)]
// pub enum DbType {
//     UInt8,
//     UInt16,
//     UInt32,
//     UInt64,
//     UInt128,
//     UInt256,
//     Int8,
//     Int16,
//     Int32,
//     Int64,
//     Int128,
//     Int256,
//     Float32,
//     Float64,
//     /// Decimal(P,S), precision [1:76], scale [0:P]
//     Decimal {
//         /// Precision
//         p: u8,
//         /// Scale
//         s: u8,
//     },
//     /// Decimal 32, P from [ 1 : 9 ]
//     Decimal32 {
//         s: u8,
//     },
//     /// Decimal 64, P from [ 10 : 18 ]
//     Decimal64 {
//         s: u8,
//     },
//     /// Decimal 128, P from [ 19 : 38 ]
//     Decimal128 {
//         s: u8,
//     },
//     /// Decimal 256, P from [ 39 : 76 ]
//     Decimal256 {
//         s: u8,
//     },
//     Boolean,
//     String,
//     FixedString {
//         n: u16,
//     },
//     Date,
//     Date32,
//     DateTime,
//     /// Date time with precision
//     DateTime64 {
//         /// Precision
//         p: usize,
//     },
//     /// UUID (16-byte number)
//     UUID,
//     /// Enum
//     Enum {
//         /// Variants (name + index)
//         variants: HashMap<String, Option<u16>>,
//     },
//     // Low cardinality
//     // TODO: implement LowCardinality type
//     // LowCardinality,
//     /// Array
//     Array {
//         /// Element type
//         elt_type: Box<DbType>,
//     },
//     /// Map
//     ///
//     /// But we simplify and only allow strings
//     Map {
//         /// Key type
//         ///
//         /// The key can be String, Integer, LowCardinality, FixedString, UUID, Date, DateTime, Date32, Enum.
//         key_type: Box<DbType>,
//         /// Value
//         value_type: Box<DbType>,
//     },
//     /// Nested
//     Nested {
//         /// Map of keys and values
//         map: HashMap<String, Box<DbType>>,
//     },
//     // Tuple
//     //
//     // TODO: implement Tuple type
//     // Tuple,
//     UInt8Nullable,
//     UInt16Nullable,
//     UInt32Nullable,
//     UInt64Nullable,
//     UInt128Nullable,
//     UInt256Nullable,
//     Int8Nullable,
//     Int16Nullable,
//     Int32Nullable,
//     Int64Nullable,
//     Int128Nullable,
//     Int256Nullable,
//     Float32Nullable,
//     Float64Nullable,
//     DecimalNullable {
//         p: u8,
//         s: u8,
//     },
//     Decimal32Nullable {
//         s: u8,
//     },
//     Decimal64Nullable {
//         s: u8,
//     },
//     Decimal128Nullable {
//         s: u8,
//     },
//     Decimal256Nullable {
//         s: u8,
//     },
//     BooleanNullable,
//     StringNullable,
//     FixedStringNullable {
//         n: u16,
//     },
//     DateNullable,
//     Date32Nullable,
//     DateTimeNullable,
//     DateTime64Nullable {
//         p: usize,
//     },
//     UUIDNullable,
// }

// impl std::fmt::Display for DbType {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let s: String = match self {
//             DbType::UInt8 => "UInt8".into(),
//             DbType::UInt16 => "UInt16".into(),
//             DbType::UInt32 => "UInt32".into(),
//             DbType::UInt64 => "UInt64".into(),
//             DbType::UInt128 => "UInt128".into(),
//             DbType::UInt256 => "UInt256".into(),
//             DbType::Int8 => "Int8".into(),
//             DbType::Int16 => "Int16".into(),
//             DbType::Int32 => "Int32".into(),
//             DbType::Int64 => "Int64".into(),
//             DbType::Int128 => "Int128".into(),
//             DbType::Int256 => "Int256".into(),
//             DbType::Float32 => "Float32".into(),
//             DbType::Float64 => "Float64".into(),
//             DbType::Decimal { p, s } => format!("Decimal({p},{s})"),
//             DbType::Decimal32 { s } => format!("Decimal32({s})"),
//             DbType::Decimal64 { s } => format!("Decimal64({s})"),
//             DbType::Decimal128 { s } => format!("Decimal128({s})"),
//             DbType::Decimal256 { s } => format!("Decimal256({s})"),
//             DbType::Boolean => "Boolean".into(),
//             DbType::String => "String".into(),
//             DbType::FixedString { n } => format!("FixedString({n})"),
//             DbType::Date => "Date".into(),
//             DbType::Date32 => "Date32".into(),
//             DbType::DateTime => "DateTime".into(),
//             DbType::DateTime64 { p } => format!("DateTime64({p})"),
//             DbType::UUID => "UUID".into(),
//             DbType::Enum { variants } => {
//                 // eg. Enum('hello' = 1, 'world' = 2)
//                 // or Enum('hello' = 1, 'world')
//                 let vars_str = variants
//                     .iter()
//                     .map(|(k, v)| {
//                         let idx = if let Some(i) = v {
//                             format!(" = {i}")
//                         } else {
//                             "".to_string()
//                         };
//                         format!("'{}'{}", k, idx)
//                     })
//                     .collect::<Vec<_>>();
//                 format!("Enum({})", vars_str.join(", "))
//             }
//             DbType::Array { elt_type } => {
//                 format!("Array({elt_type})")
//             }
//             DbType::Map {
//                 key_type,
//                 value_type,
//             } => {
//                 format!("Map({key_type},{value_type})")
//             }
//             DbType::Nested { map } => {
//                 let keyvalues_str = map
//                     .iter()
//                     .map(|(k, v)| format!("{k} {v}",))
//                     .collect::<Vec<_>>();
//                 format!("Nested({})", keyvalues_str.join(", "))
//             }
//             DbType::UInt8Nullable => "Nullable(UInt8)".into(),
//             DbType::UInt16Nullable => "Nullable(UInt16)".into(),
//             DbType::UInt32Nullable => "Nullable(UInt32)".into(),
//             DbType::UInt64Nullable => "Nullable(UInt64)".into(),
//             DbType::UInt128Nullable => "Nullable(UInt128)".into(),
//             DbType::UInt256Nullable => "Nullable(UInt256)".into(),
//             DbType::Int8Nullable => "Nullable(Int8)".into(),
//             DbType::Int16Nullable => "Nullable(Int16)".into(),
//             DbType::Int32Nullable => "Nullable(Int32)".into(),
//             DbType::Int64Nullable => "Nullable(Int64)".into(),
//             DbType::Int128Nullable => "Nullable(Int128)".into(),
//             DbType::Int256Nullable => "Nullable(Int256)".into(),
//             DbType::Float32Nullable => "Nullable(Float32)".into(),
//             DbType::Float64Nullable => "Nullable(Float64)".into(),
//             DbType::DecimalNullable { p, s } => format!("Nullable(Decimal({p},{s}))"),
//             DbType::Decimal32Nullable { s } => format!("Nullable(Decimal32({s}))"),
//             DbType::Decimal64Nullable { s } => format!("Nullable(Decimal64({s}))"),
//             DbType::Decimal128Nullable { s } => format!("Nullable(Decimal128({s}))"),
//             DbType::Decimal256Nullable { s } => format!("Nullable(Decimal256({s}))"),
//             DbType::BooleanNullable => "Nullable(Boolean)".into(),
//             DbType::StringNullable => "Nullable(String)".into(),
//             DbType::FixedStringNullable { n } => format!("Nullable(FixedString({n}))"),
//             DbType::DateNullable => "Nullable(Date)".into(),
//             DbType::Date32Nullable => "Nullable(Date32)".into(),
//             DbType::DateTimeNullable => "Nullable(DateTime)".into(),
//             DbType::DateTime64Nullable { p } => format!("Nullable(DateTime64({p}))"),
//             DbType::UUIDNullable => "Nullable(UUID)".into(),
//         };
//         write!(f, "{s}")
//     }
// }

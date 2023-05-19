//! Data types

use std::collections::HashMap;

/// Extension trait to map a Rust type as a DB type
pub trait DbTypeExt {
    /// DB type
    const DB_TYPE: DbType;
}

/// Clickhouse data type
#[derive(Debug)]
pub enum DbType {
    /// Unsigned integer 8
    UInt8 {
        /// Nullable
        nullable: bool,
    },
    /// Unsigned integer 16
    UInt16 {
        /// Nullable
        nullable: bool,
    },
    /// Unsigned integer 32
    UInt32 {
        /// Nullable
        nullable: bool,
    },
    /// Unsigned integer 64
    UInt64 {
        /// Nullable
        nullable: bool,
    },
    /// Unsigned integer 128
    UInt128 {
        /// Nullable
        nullable: bool,
    },
    /// Unsigned integer 256
    UInt256 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 8
    Int8 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 16
    Int16 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 32
    Int32 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 64
    Int64 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 128
    Int128 {
        /// Nullable
        nullable: bool,
    },
    /// Signed integer 256
    Int256 {
        /// Nullable
        nullable: bool,
    },
    /// Float 32
    Float32 {
        /// Nullable
        nullable: bool,
    },
    /// FLoat 64
    Float64 {
        /// Nullable
        nullable: bool,
    },
    /// Decimal(P,S), precision [1:76], scale [0:P]
    Decimal {
        /// Precision
        precision: u8,
        /// Scale
        scale: u8,
        /// Nullable
        nullable: bool,
    },
    /// Decimal 32, P from [ 1 : 9 ]
    Decimal32 {
        /// Scale
        scale: u8,
        /// Nullable
        nullable: bool,
    },
    /// Decimal 64, P from [ 10 : 18 ]
    Decimal64 {
        /// Scale
        scale: u8,
        /// Nullable
        nullable: bool,
    },
    /// Decimal 128, P from [ 19 : 38 ]
    Decimal128 {
        /// Scale
        scale: u8,
        /// Nullable
        nullable: bool,
    },
    /// Decimal 256, P from [ 39 : 76 ]
    Decimal256 {
        /// Scale
        scale: u8,
        /// Nullable
        nullable: bool,
    },
    /// Boolean
    Boolean {
        /// Nullable
        nullable: bool,
    },
    /// String
    String {
        /// Nullable
        nullable: bool,
    },
    /// Fixed string
    FixedString {
        /// Size
        size: u16,
        /// Nullable
        nullable: bool,
    },
    /// Date (2 bytes)
    Date {
        /// Nullable
        nullable: bool,
    },
    /// Same range as Date32 (stored as u32)
    Date32 {
        /// Nullable
        nullable: bool,
    },
    /// Date time
    DateTime {
        /// Nullable
        nullable: bool,
    },
    /// Date time with precision
    DateTime64 {
        /// Precision
        precision: usize,
        /// Nullable
        nullable: bool,
    },
    /// UUID (16-byte number)
    UUID {
        /// Nullable
        nullable: bool,
    },
    /// Enum
    Enum {
        /// Values (name + index)
        values: HashMap<String, Option<u16>>,
    },
    // Low cardinality
    // TODO: implement LowCardinality type
    // LowCardinality,
    /// Array
    Array {
        /// Type
        value_type: Box<DbType>,
    },
    /// Map
    ///

    /// But we simplify and only allow strings
    Map {
        /// Key type
        ///
        /// The key can be String, Integer, LowCardinality, FixedString, UUID, Date, DateTime, Date32, Enum.
        key_type: Box<DbType>,
        /// Value
        value_type: Box<DbType>,
    },
    /// Nested
    Nested {
        /// Keys + values
        keyvalues: HashMap<String, Box<DbType>>,
    },
    // Tuple
    //
    // TODO: implement Tuple type
    // Tuple,
}

impl DbType {
    /// Returns the type size in bytes (None if variable length)
    pub fn size(&self) -> Option<usize> {
        match self {
            DbType::UInt8 { nullable: _ } => Some(1),
            DbType::UInt16 { nullable: _ } => Some(2),
            DbType::UInt32 { nullable: _ } => Some(4),
            DbType::UInt64 { nullable: _ } => Some(8),
            DbType::UInt128 { nullable: _ } => Some(16),
            DbType::UInt256 { nullable: _ } => Some(32),
            DbType::Int8 { nullable: _ } => Some(1),
            DbType::Int16 { nullable: _ } => Some(2),
            DbType::Int32 { nullable: _ } => Some(4),
            DbType::Int64 { nullable: _ } => Some(8),
            DbType::Int128 { nullable: _ } => Some(16),
            DbType::Int256 { nullable: _ } => Some(32),
            DbType::Float32 { nullable: _ } => None,
            DbType::Float64 { nullable: _ } => None,
            DbType::Decimal {
                precision: _,
                scale: _,
                nullable: _,
            } => None,
            DbType::Decimal32 {
                scale: _,
                nullable: _,
            } => None,
            DbType::Decimal64 {
                scale: _,
                nullable: _,
            } => None,
            DbType::Decimal128 {
                scale: _,
                nullable: _,
            } => None,
            DbType::Decimal256 {
                scale: _,
                nullable: _,
            } => None,
            DbType::Boolean { nullable: _ } => Some(1),
            DbType::String { nullable: _ } => None,
            DbType::FixedString { size, nullable: _ } => Some(*size as usize),
            DbType::Date { nullable: _ } => Some(2),
            DbType::Date32 { nullable: _ } => Some(4),
            DbType::DateTime { nullable: _ } => Some(4),
            DbType::DateTime64 {
                precision: _,
                nullable: _,
            } => Some(8),
            DbType::UUID { nullable: _ } => Some(16),
            DbType::Enum { values: _ } => None,
            DbType::Array { value_type: _ } => None,
            DbType::Map {
                key_type: _,
                value_type: _,
            } => None,
            DbType::Nested { keyvalues: _ } => None,
        }
    }
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DbType::UInt8 { nullable } => {
                if !*nullable {
                    "UInt8".to_string()
                } else {
                    "Nullable(UInt8)".to_string()
                }
            }
            DbType::UInt16 { nullable } => {
                if !*nullable {
                    "UInt16".to_string()
                } else {
                    "Nullable(UInt16)".to_string()
                }
            }
            DbType::UInt32 { nullable } => {
                if !*nullable {
                    "UInt32".to_string()
                } else {
                    "Nullable(UInt32)".to_string()
                }
            }
            DbType::UInt64 { nullable } => {
                if !*nullable {
                    "UInt64".to_string()
                } else {
                    "Nullable(UInt64)".to_string()
                }
            }
            DbType::UInt128 { nullable } => {
                if !*nullable {
                    "UInt128".to_string()
                } else {
                    "Nullable(UInt128)".to_string()
                }
            }
            DbType::UInt256 { nullable } => {
                if !*nullable {
                    "UInt256".to_string()
                } else {
                    "Nullable(UInt256)".to_string()
                }
            }
            DbType::Int8 { nullable } => {
                if !*nullable {
                    "Int8".to_string()
                } else {
                    "Nullable(Int8)".to_string()
                }
            }
            DbType::Int16 { nullable } => {
                if !*nullable {
                    "Int16".to_string()
                } else {
                    "Nullable(Int16)".to_string()
                }
            }
            DbType::Int32 { nullable } => {
                if !*nullable {
                    "Int32".to_string()
                } else {
                    "Nullable(Int32)".to_string()
                }
            }
            DbType::Int64 { nullable } => {
                if !*nullable {
                    "Int64".to_string()
                } else {
                    "Nullable(Int64)".to_string()
                }
            }
            DbType::Int128 { nullable } => {
                if !*nullable {
                    "Int128".to_string()
                } else {
                    "Nullable(Int128)".to_string()
                }
            }
            DbType::Int256 { nullable } => {
                if !*nullable {
                    "Int256".to_string()
                } else {
                    "Nullable(Int256)".to_string()
                }
            }
            DbType::Float32 { nullable } => {
                if !*nullable {
                    "Float32".to_string()
                } else {
                    "Nullable(Float32)".to_string()
                }
            }
            DbType::Float64 { nullable } => {
                if !*nullable {
                    "Float64".to_string()
                } else {
                    "Nullable(Float64)".to_string()
                }
            }
            DbType::Decimal {
                precision,
                scale,
                nullable,
            } => {
                if !*nullable {
                    format!("Decimal({},{})", precision, scale)
                } else {
                    format!("Nullable(Decimal({},{}))", precision, scale)
                }
            }
            DbType::Decimal32 { scale, nullable } => {
                if !*nullable {
                    format!("Decimal32({})", scale)
                } else {
                    format!("Nullable(Decimal32({}))", scale)
                }
            }
            DbType::Decimal64 { scale, nullable } => {
                if !*nullable {
                    format!("Decimal64({})", scale)
                } else {
                    format!("Nullable(Decimal64({}))", scale)
                }
            }
            DbType::Decimal128 { scale, nullable } => {
                if !*nullable {
                    format!("Decimal128({})", scale)
                } else {
                    format!("Nullable(Decimal128({}))", scale)
                }
            }
            DbType::Decimal256 { scale, nullable } => {
                if !*nullable {
                    format!("Decimal256({})", scale)
                } else {
                    format!("Nullable(Decimal256({}))", scale)
                }
            }
            DbType::Boolean { nullable } => {
                if !*nullable {
                    "Boolean".to_string()
                } else {
                    "Nullable(Boolean)".to_string()
                }
            }
            DbType::String { nullable } => {
                if !*nullable {
                    "String".to_string()
                } else {
                    "Nullable(String)".to_string()
                }
            }
            DbType::FixedString { size, nullable } => {
                if !*nullable {
                    format!("FixedString({})", size)
                } else {
                    format!("Nullable(FixedString({}))", size)
                }
            }
            DbType::Date { nullable } => {
                if !*nullable {
                    "Date".to_string()
                } else {
                    "Nullable(Date)".to_string()
                }
            }
            DbType::Date32 { nullable } => {
                if !*nullable {
                    "Date32".to_string()
                } else {
                    "Nullable(Date32)".to_string()
                }
            }
            DbType::DateTime { nullable } => {
                if !*nullable {
                    "DateTime".to_string()
                } else {
                    "Nullable(DateTime)".to_string()
                }
            }
            DbType::DateTime64 {
                precision,
                nullable,
            } => {
                if !*nullable {
                    format!("DateTime64({})", precision)
                } else {
                    format!("Nullable(DateTime64({}))", precision)
                }
            }
            DbType::UUID { nullable } => {
                if !*nullable {
                    "UUID".to_string()
                } else {
                    "Nullable(UUID)".to_string()
                }
            }
            DbType::Enum { values } => {
                // eg. Enum('hello' = 1, 'world' = 2)
                // or Enum('hello' = 1, 'world')
                let values_str = values
                    .iter()
                    .map(|(k, v)| {
                        let idx = if let Some(i) = v {
                            format!(" = {i}")
                        } else {
                            "".to_string()
                        };
                        format!("'{}'{}", k, idx)
                    })
                    .collect::<Vec<_>>();
                format!("Enum({})", values_str.join(", "))
            }
            DbType::Array { value_type } => format!("Array({value_type})"),
            DbType::Map {
                key_type,
                value_type,
            } => format!("Map({key_type},{value_type})"),
            DbType::Nested { keyvalues } => {
                let keyvalues_str = keyvalues
                    .iter()
                    .map(|(k, v)| format!("{k} {v}",))
                    .collect::<Vec<_>>();
                format!("Nested({})", keyvalues_str.join(", "))
            }
        };
        write!(f, "{s}")
    }
}

// TYPE IMPLEMENTATION

/// Implements a base type
macro_rules! impl_base_type {
    ($TY:ty, $DB_TY:tt, $NULL:expr) => {
        impl DbTypeExt for $TY {
            const DB_TYPE: DbType = DbType::$DB_TY { nullable: $NULL };
        }
    };
}

impl_base_type!(u8, UInt8, false);
impl_base_type!(u16, UInt16, false);
impl_base_type!(u32, UInt32, false);
impl_base_type!(u64, UInt64, false);
impl_base_type!(u128, UInt128, false);
impl_base_type!(usize, UInt64, false);
impl_base_type!(i8, Int8, false);
impl_base_type!(i16, Int16, false);
impl_base_type!(i32, Int32, false);
impl_base_type!(i64, Int64, false);
impl_base_type!(i128, Int128, false);
impl_base_type!(isize, Int64, false);
impl_base_type!(bool, Boolean, false);
impl_base_type!(String, String, false);

// implement for Option<?>
impl_base_type!(Option<u8>, UInt8, true);
impl_base_type!(Option<u16>, UInt16, true);
impl_base_type!(Option<u32>, UInt32, true);
impl_base_type!(Option<u64>, UInt64, true);
impl_base_type!(Option<u128>, UInt128, true);
impl_base_type!(Option<usize>, UInt64, true);
impl_base_type!(Option<i8>, Int8, true);
impl_base_type!(Option<i16>, Int16, true);
impl_base_type!(Option<i32>, Int32, true);
impl_base_type!(Option<i64>, Int64, true);
impl_base_type!(Option<i128>, Int128, true);
impl_base_type!(Option<isize>, Int64, true);
impl_base_type!(Option<bool>, Boolean, true);
impl_base_type!(Option<String>, String, true);

// implement for references
impl<'a, T> DbTypeExt for &'a T
where
    T: DbTypeExt,
{
    const DB_TYPE: DbType = T::DB_TYPE;
}

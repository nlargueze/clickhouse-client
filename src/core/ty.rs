//! Type struct

use std::str::FromStr;

use once_cell::sync::OnceCell;
use regex::Regex;

use crate::error::Error;

/// Data type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    /// UInt8
    UInt8,
    /// UInt16
    UInt16,
    /// UInt32
    UInt32,
    /// UInt64
    UInt64,
    /// UInt128
    UInt128,
    /// UInt256
    UInt256,
    /// Int8
    Int8,
    /// UInt16
    Int16,
    /// UInt32
    Int32,
    /// UInt64
    Int64,
    /// UInt128
    Int128,
    /// UInt256
    Int256,
    /// Float32
    Float32,
    /// UInt64
    Float64,
    /// Decimal (precision ∈ [1:76], scale ∈ [0:P])
    Decimal(u8, u8),
    /// Decimal32
    Decimal32(u8),
    /// Decimal64
    Decimal64(u8),
    /// Decimal128
    Decimal128(u8),
    /// Decimal256
    Decimal256(u8),
    /// Boolean
    Bool,
    /// String
    String,
    /// Fixed string
    FixedString(usize),
    /// Date (number of days since 1970-01-01, 2 bytes)
    Date,
    /// Date32 (number of days since 1970-01-01, signed i32)
    Date32,
    /// DateTime (seconds since EPOCH, [1970-01-01 00:00:00, 2106-02-07 06:28:15])
    DateTime,
    /// Ticks since since epoch start (1970-01-01 00:00:00 UTC)
    ///
    /// Precision [0:9] defines the resolution, eg 3=ms, 6=us, 9=ns
    DateTime64(u8),
    /// Enum (256 values, i8)
    ///
    /// Keys and indices must be unique
    Enum8(Vec<(String, Option<i8>)>),
    /// Enum (65536 values, i16)
    ///
    /// Keys and indices must be unique
    Enum16(Vec<(String, Option<i16>)>),
    /// UUID (16 bytes)
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
    Nested(Vec<(String, Box<Type>)>),
    /// Tuple
    ///
    /// Each element can have a different type
    Tuple(Vec<Box<Type>>),
    /// Nullable UInt8
    NullableUInt8,
    /// Nullable UInt16
    NullableUInt16,
    /// Nullable UInt32
    NullableUInt32,
    /// Nullable UInt64
    NullableUInt64,
    /// Nullable UInt128
    NullableUInt128,
    /// Nullable UInt256
    NullableUInt256,
    /// Nullable Int8
    NullableInt8,
    /// Nullable Int16
    NullableInt16,
    /// Nullable Int32
    NullableInt32,
    /// Nullable Int64
    NullableInt64,
    /// Nullable Int128
    NullableInt128,
    /// Nullable Int256
    NullableInt256,
    /// Nullable Float32
    NullableFloat32,
    /// Nullable Float64
    NullableFloat64,
    /// Nullable Decimal
    NullableDecimal(u8, u8),
    /// Nullable Decimal32
    NullableDecimal32(u8),
    /// Nullable Decimal64
    NullableDecimal64(u8),
    /// Nullable Decimal128
    NullableDecimal128(u8),
    /// Nullable Decimal256
    NullableDecimal256(u8),
    /// Nullable Bool
    NullableBool,
    /// Nullable String
    NullableString,
    /// Nullable FixedString
    NullableFixedString(usize),
    /// Nullable Date
    NullableDate,
    /// Nullable Date32
    NullableDate32,
    /// Nullable DateTime
    NullableDateTime,
    /// Nullable DateTime64
    NullableDateTime64(u8),
    /// Enum (256 values)
    ///
    /// Keys and indices must be unique
    NullableEnum8(Vec<(String, Option<i8>)>),
    /// Enum (65536 values)
    ///
    /// Keys and indices must be unique
    NullableEnum16(Vec<(String, Option<i16>)>),
    /// Nullable UUID
    NullableUUID,
}

impl Type {
    /// Returns the nullable variant
    ///
    /// # Errors
    ///
    /// There is an error if the type is not nullable
    fn nullable_variant(&self) -> Result<Type, Error> {
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
            Type::Decimal(s, p) => Ok(Type::NullableDecimal(*s, *p)),
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
            Type::Enum8(map) => Ok(Type::NullableEnum8(map.clone())),
            Type::Enum16(map) => Ok(Type::NullableEnum16(map.clone())),
            Type::UUID => Ok(Type::NullableUUID),
            Type::Array(_)
            | Type::Map(_, _)
            | Type::Nested(_)
            | Type::Tuple(_)
            | Type::NullableUInt8
            | Type::NullableUInt16
            | Type::NullableUInt32
            | Type::NullableUInt64
            | Type::NullableUInt128
            | Type::NullableUInt256
            | Type::NullableInt8
            | Type::NullableInt16
            | Type::NullableInt32
            | Type::NullableInt64
            | Type::NullableInt128
            | Type::NullableInt256
            | Type::NullableFloat32
            | Type::NullableFloat64
            | Type::NullableDecimal(_, _)
            | Type::NullableDecimal32(_)
            | Type::NullableDecimal64(_)
            | Type::NullableDecimal128(_)
            | Type::NullableDecimal256(_)
            | Type::NullableBool
            | Type::NullableString
            | Type::NullableFixedString(_)
            | Type::NullableDate
            | Type::NullableDate32
            | Type::NullableDateTime
            | Type::NullableDateTime64(_)
            | Type::NullableEnum8(_)
            | Type::NullableEnum16(_)
            | Type::NullableUUID => Err(Error(format!("Type {} is not nullable", self))),
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
            Type::Enum8(vars) => {
                format!(
                    "Enum8({})",
                    vars.iter()
                        .map(|(key, idx)| {
                            let idx_str = match idx {
                                Some(i) => format!(" = {i}"),
                                None => "".to_string(),
                            };
                            format!("'{key}'{idx_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::Enum16(vars) => {
                format!(
                    "Enum16({})",
                    vars.iter()
                        .map(|(key, idx)| {
                            let idx_str = match idx {
                                Some(i) => format!(" = {i}"),
                                None => "".to_string(),
                            };
                            format!("'{key}'{idx_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::UUID => "UUID".into(),
            Type::Array(t) => format!("Array({t})"),
            Type::Map(k, v) => format!("Map({k}, {v})"),
            Type::Tuple(types) => {
                format!(
                    "Tuple({})",
                    types
                        .iter()
                        .map(|ty| { format!("{ty}") })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::Nested(fields) => {
                format!(
                    "Nested({})",
                    fields
                        .iter()
                        .map(|(name, ty)| { format!("{} {}", name, ty) })
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
            Type::NullableEnum8(vars) => {
                format!(
                    "Nullable(Enum8({}))",
                    vars.iter()
                        .map(|(key, idx)| {
                            let idx_str = match idx {
                                Some(i) => format!(" = {i}"),
                                None => "".to_string(),
                            };
                            format!("'{key}'{idx_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::NullableEnum16(vars) => {
                format!(
                    "Nullable(Enum16({}))",
                    vars.iter()
                        .map(|(key, idx)| {
                            let idx_str = match idx {
                                Some(i) => format!(" = {i}"),
                                None => "".to_string(),
                            };
                            format!("'{key}'{idx_str}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::NullableUUID => "Nullable(UUID)".into(),
        };
        write!(f, "{s}")
    }
}

impl FromStr for Type {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // simple types
        match s {
            "UInt8" => return Ok(Type::UInt8),
            "UInt16" => return Ok(Type::UInt16),
            "UInt32" => return Ok(Type::UInt32),
            "UInt64" => return Ok(Type::UInt64),
            "UInt128" => return Ok(Type::UInt128),
            "UInt256" => return Ok(Type::UInt256),
            "Int8" => return Ok(Type::Int8),
            "Int16" => return Ok(Type::Int16),
            "Int32" => return Ok(Type::Int32),
            "Int64" => return Ok(Type::Int64),
            "Int128" => return Ok(Type::Int128),
            "Int256" => return Ok(Type::Int256),
            "Float32" => return Ok(Type::Float32),
            "Float64" => return Ok(Type::Float64),
            "Bool" => return Ok(Type::Bool),
            "String" => return Ok(Type::String),
            "Date" => return Ok(Type::Date),
            "Date32" => return Ok(Type::Date32),
            "DateTime" => return Ok(Type::DateTime),
            "UUID" => return Ok(Type::UUID),
            _ => {}
        }

        // Hint that the type is nullable
        if s.starts_with("Nullable") {
            /// A regex for nullable types
            static REGEX_NULLABLE: OnceCell<Regex> = OnceCell::new();

            let regex_nullable = REGEX_NULLABLE
                .get_or_init(|| Regex::new(r"Nullable\((?P<ty>[[:word:]]+)\)").unwrap());
            if let Some(caps) = regex_nullable.captures(s) {
                if let Some(m) = caps.name("ty") {
                    // Nullable is parsed only at the beginning of the string
                    eprintln!("YYY={:?}", m.range());
                    // Nullable should end at the end of the string
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("Type '{s}' is nullable ')' ")));
                    }
                    let inner_ty = m.as_str().parse::<Type>()?;
                    return inner_ty.nullable_variant();
                }
            }
        }

        // Hint for decimal 32
        if s.starts_with("Decimal32") {
            /// A regex for Decimal32(s)
            static REGEX_DECIMAL32: OnceCell<Regex> = OnceCell::new();

            let regex_dec32 = REGEX_DECIMAL32
                .get_or_init(|| Regex::new(r"Decimal32\((?P<scale>\d{1})\)").unwrap());
            if let Some(caps) = regex_dec32.captures(s) {
                if let Some(m) = caps.name("scale") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Decimal32 type: {s}")));
                    }
                    let scale: u8 = m.as_str().parse()?;
                    return Ok(Type::Decimal32(scale));
                }
            } else {
                return Err(Error(format!("invalid Decimal32 type: {s}")));
            }
        }

        // Hint for decimal 64
        if s.starts_with("Decimal64") {
            /// A regex for Decimal64(s)
            static REGEX_DECIMAL64: OnceCell<Regex> = OnceCell::new();

            let regex_dec64 = REGEX_DECIMAL64
                .get_or_init(|| Regex::new(r"Decimal64\((?P<scale>\d{1})\)").unwrap());
            if let Some(caps) = regex_dec64.captures(s) {
                if let Some(m) = caps.name("scale") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Decimal64 type: {s}")));
                    }
                    let scale: u8 = m.as_str().parse()?;
                    return Ok(Type::Decimal64(scale));
                }
            }
        }

        // Hint for decimal 128
        if s.starts_with("Decimal128") {
            /// A regex for Decimal128(s)
            static REGEX_DECIMAL128: OnceCell<Regex> = OnceCell::new();

            let regex_dec128 = REGEX_DECIMAL128
                .get_or_init(|| Regex::new(r"Decimal128\((?P<scale>\d{1})\)").unwrap());
            if let Some(caps) = regex_dec128.captures(s) {
                if let Some(m) = caps.name("scale") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Decimal128 type: {s}")));
                    }

                    let scale: u8 = m.as_str().parse()?;
                    return Ok(Type::Decimal128(scale));
                }
            }
        }

        // Hint for decimal 128
        if s.starts_with("Decimal256") {
            /// A regex for Decimal256(s)
            static REGEX_DECIMAL256: OnceCell<Regex> = OnceCell::new();

            let regex_dec256 = REGEX_DECIMAL256
                .get_or_init(|| Regex::new(r"Decimal256\((?P<scale>\d{1})\)").unwrap());
            if let Some(caps) = regex_dec256.captures(s) {
                if let Some(m) = caps.name("scale") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Decimal256 type: {s}")));
                    }
                    let scale: u8 = m.as_str().parse()?;
                    return Ok(Type::Decimal256(scale));
                }
            }
        }

        // Hint for decimal
        if s.starts_with("Decimal") {
            /// A regex for Decimal(s)
            static REGEX_DECIMAL: OnceCell<Regex> = OnceCell::new();

            let regex_dec = REGEX_DECIMAL.get_or_init(|| {
                Regex::new(r"Decimal\((?P<prec>\d{1}),(?P<scale>\d{1})\)").unwrap()
            });
            if let Some(caps) = regex_dec.captures(s) {
                let p = if let Some(m) = caps.name("prec") {
                    let precision: u8 = m.as_str().parse()?;
                    precision
                } else {
                    return Err(Error(format!("invalid Decimal type: {s}")));
                };
                let s = if let Some(m) = caps.name("scale") {
                    let scale: u8 = m.as_str().parse()?;
                    scale
                } else {
                    return Err(Error(format!("invalid Decimal type: {s}")));
                };
                return Ok(Type::Decimal(p, s));
            }
        }

        // Hint for FixedString
        if s.starts_with("FixedString") {
            /// A regex for FixedString(n)
            static REGEX_FIXEDSTRING: OnceCell<Regex> = OnceCell::new();

            let regex_fixestring = REGEX_FIXEDSTRING
                .get_or_init(|| Regex::new(r"FixedString\((?P<size>\d{1})\)").unwrap());
            if let Some(caps) = regex_fixestring.captures(s) {
                if let Some(m) = caps.name("size") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid FixedString type: {s}")));
                    }
                    let n: usize = m.as_str().parse()?;
                    return Ok(Type::FixedString(n));
                }
            }
        }

        // Hint for DateTime64
        if s.starts_with("DateTime64") {
            /// A regex for DateTime64
            static REGEX_DATETIME64: OnceCell<Regex> = OnceCell::new();

            let regex_datetime64 = REGEX_DATETIME64
                .get_or_init(|| Regex::new(r"DateTime64\((?P<precision>\d{1})\)").unwrap());
            if let Some(caps) = regex_datetime64.captures(s) {
                if let Some(m) = caps.name("precision") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid DateTime64 type: {s}")));
                    }
                    let i: u8 = m.as_str().parse()?;
                    return Ok(Type::DateTime64(i));
                }
            }
        }

        // Hint for Enum8
        if s.starts_with("Enum8") {
            /// Regex for ENUM8
            static REGEX_ENUM8: OnceCell<Regex> = OnceCell::new();

            let regex_enum8 =
                REGEX_ENUM8.get_or_init(|| Regex::new(r"Enum8\((?P<items>.*)\)").unwrap());
            if let Some(caps) = regex_enum8.captures(s) {
                if let Some(m) = caps.name("items") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Enum8 type: {s}")));
                    }

                    /// Regex for ENUM items
                    static REGEX_ENUM_ITEMS: OnceCell<Regex> = OnceCell::new();

                    let regex_items = REGEX_ENUM_ITEMS.get_or_init(|| {
                        Regex::new(r"((?P<key>'.*?')\s*?=?\s*?(?P<id>\d+)\s*?,?\s*?)+").unwrap()
                    });
                    let mut items = vec![];
                    for caps in regex_items.captures_iter(s) {
                        let key = if let Some(m) = caps.name("key") {
                            let key = m.as_str();
                            let key = key.strip_prefix('\'').unwrap_or(key);
                            let key = key.strip_suffix('\'').unwrap_or(key);
                            key.to_string()
                        } else {
                            return Err(Error("invalid Enum8 items".to_string()));
                        };
                        let id = if let Some(m) = caps.name("id") {
                            let id_str = m.as_str();
                            let i: i8 = id_str.parse()?;
                            Some(i)
                        } else {
                            None
                        };
                        items.push((key, id));
                    }
                    if items.is_empty() {
                        return Err(Error("invalid Enum8 items".to_string()));
                    }
                    return Ok(Type::Enum8(items));
                }
            }
        }

        // Hint for Enum16
        if s.starts_with("Enum16") {
            /// Regex for ENUM16
            static REGEX_ENUM16: OnceCell<Regex> = OnceCell::new();

            let regex_enum16 =
                REGEX_ENUM16.get_or_init(|| Regex::new(r"Enum16\((?P<items>.*)\)").unwrap());
            if let Some(caps) = regex_enum16.captures(s) {
                if let Some(m) = caps.name("items") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Enum16 type: {s}")));
                    }

                    /// Regex for ENUM items
                    static REGEX_ENUM_ITEMS: OnceCell<Regex> = OnceCell::new();

                    let regex_items = REGEX_ENUM_ITEMS.get_or_init(|| {
                        Regex::new(r"((?P<key>'.*?')\s*?=?\s*?(?P<id>\d+)\s*?,?\s*?)+").unwrap()
                    });
                    let mut items = vec![];
                    for caps in regex_items.captures_iter(s) {
                        let key = if let Some(m) = caps.name("key") {
                            let key = m.as_str();
                            let key = key.strip_prefix('\'').unwrap_or(key);
                            let key = key.strip_suffix('\'').unwrap_or(key);
                            key.to_string()
                        } else {
                            return Err(Error("invalid Enum16 items".to_string()));
                        };
                        let id = if let Some(m) = caps.name("id") {
                            let id_str = m.as_str();
                            let i: i16 = id_str.parse()?;
                            Some(i)
                        } else {
                            None
                        };
                        items.push((key, id));
                    }
                    if items.is_empty() {
                        return Err(Error("invalid Enum16 items".to_string()));
                    }
                    return Ok(Type::Enum16(items));
                }
            }
        }

        // Hint for Array
        if s.starts_with("Array") {
            /// A regex for arrays
            static REGEX_ARRAY: OnceCell<Regex> = OnceCell::new();

            let regex_array =
                REGEX_ARRAY.get_or_init(|| Regex::new(r"Array\((?P<ty>.+)\)").unwrap());
            if let Some(caps) = regex_array.captures(s) {
                if let Some(m) = caps.name("ty") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Array type: {s}")));
                    }
                    let ty: Type = m.as_str().parse()?;
                    return Ok(Type::Array(Box::new(ty)));
                }
            }
        }

        // Hint for Map
        if s.starts_with("Map") {
            /// A regex for maps
            static REGEX_MAP: OnceCell<Regex> = OnceCell::new();

            let regex_map = REGEX_MAP.get_or_init(|| Regex::new(r"Map\((?P<items>.*)\)").unwrap());
            if let Some(caps) = regex_map.captures(s) {
                if let Some(m) = caps.name("items") {
                    if m.end() + 1 < s.len() {
                        return Err(Error(format!("invalid Map type: {s}")));
                    }
                    let types = m.as_str();

                    /// Regex for MAP items
                    static REGEX_MAP_ITEMS: OnceCell<Regex> = OnceCell::new();

                    let regex_items: &Regex = REGEX_MAP_ITEMS.get_or_init(|| {
                        Regex::new(r"(?P<key>[[:word:]]+)\s*,?\s*?(?P<val>[[:word:]]+)").unwrap()
                    });
                    if let Some(caps) = regex_items.captures(types) {
                        let key_ty = if let Some(m) = caps.name("key") {
                            let key = m.as_str();
                            key.to_string().parse()?
                        } else {
                            return Err(Error("invalid Map items".to_string()));
                        };
                        let val_ty = if let Some(m) = caps.name("val") {
                            let val_str = m.as_str();
                            val_str.parse()?
                        } else {
                            return Err(Error("invalid Map items".to_string()));
                        };
                        return Ok(Type::Map(Box::new(key_ty), Box::new(val_ty)));
                    }
                }
            }
        }

        // TODO: Tuple

        Err(Error(format!("Type '{s}' is not a valid type")))
    }
}

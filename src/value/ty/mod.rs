//! Data types

#[cfg(test)]
mod tests;

use std::{collections::BTreeMap, str::FromStr};

use crate::error::Error;

/// Data type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    /// u8
    UInt8,
    /// u16
    UInt16,
    /// u32
    UInt32,
    /// u64
    UInt64,
    /// u128
    UInt128,
    /// u256
    UInt256,
    /// i8
    Int8,
    /// i16
    Int16,
    /// i32
    Int32,
    /// i64
    Int64,
    /// i128
    Int128,
    /// i256
    Int256,
    /// f32
    Float32,
    /// f64
    Float64,
    /// Decimal(P,S) (precision ∈ [1:76], scale ∈ [0:P], range ( -1 * 10^(P - S), 1 * 10^(P - S) )
    Decimal(u8, u8),
    /// Decimal(P ∈ [1:9])
    Decimal32(u8),
    /// Decimal(P ∈ [19:18])
    Decimal64(u8),
    /// Decimal(P ∈ [19:38])
    Decimal128(u8),
    /// Decimal(P ∈ [39:76])
    Decimal256(u8),
    /// Boolean
    Bool,
    /// String
    String,
    /// Fixed string
    FixedString(u8),
    /// UUID (16 bytes)
    UUID,
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
    Enum8(BTreeMap<String, i8>),
    /// Enum (65536 values, i16)
    ///
    /// Keys and indices must be unique
    Enum16(BTreeMap<String, i16>),
    /// Array
    ///
    /// An array element can have any type
    Array(Box<Type>),
    /// Tuple
    ///
    /// Each element can have a different type
    Tuple(Vec<Type>),
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
    Nested(Vec<(String, Type)>),
    /// Nullable u8
    NullableUInt8,
    /// Nullable u16
    NullableUInt16,
    /// Nullable u32
    NullableUInt32,
    /// Nullable u64
    NullableUInt64,
    /// Nullable u128
    NullableUInt128,
    /// Nullable u256
    NullableUInt256,
    /// Nullable i8
    NullableInt8,
    /// Nullable i16
    NullableInt16,
    /// Nullable i32
    NullableInt32,
    /// Nullable i64
    NullableInt64,
    /// Nullable i128
    NullableInt128,
    /// Nullable i256
    NullableInt256,
    /// Nullable f32
    NullableFloat32,
    /// Nullable f64
    NullableFloat64,
    /// Nullable decimal
    NullableDecimal(u8, u8),
    /// Nullable decimal32
    NullableDecimal32(u8),
    /// Nullable decimal64
    NullableDecimal64(u8),
    /// Nullable decimal128
    NullableDecimal128(u8),
    /// Nullable decimal256
    NullableDecimal256(u8),
    /// Nullable bool
    NullableBool,
    /// Nullable string
    NullableString,
    /// Nullable fixed string
    NullableFixedString(u8),
    /// Nullbale UUID
    NullableUUID,
    /// Nullable date
    NullableDate,
    /// Nullable date32
    NullableDate32,
    /// Nullable datetime
    NullableDateTime,
    /// Nullable datetime64
    NullableDateTime64(u8),
    /// Nullable Enum8
    NullableEnum8(BTreeMap<String, i8>),
    /// Nullable Enum16
    NullableEnum16(BTreeMap<String, i16>),
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
            Type::Enum8(vars) => {
                format!(
                    "Enum8({})",
                    vars.iter()
                        .map(|(key, idx)| { format!("'{key}' = {idx}") })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Type::Enum16(vars) => {
                format!(
                    "Enum16({})",
                    vars.iter()
                        .map(|(key, idx)| { format!("'{key}' = {idx}") })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
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
            Type::NullableEnum8(keys) => format!("Nullable({})", Type::Enum8(keys.clone())),
            Type::NullableEnum16(keys) => format!("Nullable({})", Type::Enum16(keys.clone())),
        };

        write!(f, "{s}")
    }
}

impl FromStr for Type {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // base types
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
            "UUID" => return Ok(Type::UUID),
            "Date" => return Ok(Type::Date),
            "Date32" => return Ok(Type::Date32),
            "DateTime" => return Ok(Type::DateTime),
            _ => {}
        }

        // > Decimal(P, S)
        if let Some(s) = s.strip_prefix("Decimal(") {
            if let Some(s) = s.strip_suffix(')') {
                let parts = s.split(',').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(Error::new("invalid Decimal type"));
                }
                let p = parts[0].trim().parse::<u8>()?;
                let s = parts[1].trim().parse::<u8>()?;
                return Ok(Type::Decimal(p, s));
            } else {
                return Err(Error::new("invalid Decimal type"));
            }
        }

        // > Decimal32(S)
        if let Some(s) = s.strip_prefix("Decimal32(") {
            if let Some(s) = s.strip_suffix(')') {
                let s = s.trim().parse::<u8>()?;
                return Ok(Type::Decimal32(s));
            } else {
                return Err(Error::new("invalid Decimal32 type"));
            }
        }

        // > Decimal64(S)
        if let Some(s) = s.strip_prefix("Decimal64(") {
            if let Some(s) = s.strip_suffix(')') {
                let s = s.trim().parse::<u8>()?;
                return Ok(Type::Decimal64(s));
            } else {
                return Err(Error::new("invalid Decimal64 type"));
            }
        }

        // > Decimal128(S)
        if let Some(s) = s.strip_prefix("Decimal128(") {
            if let Some(s) = s.strip_suffix(')') {
                let s = s.trim().parse::<u8>()?;
                return Ok(Type::Decimal128(s));
            } else {
                return Err(Error::new("invalid Decimal128 type"));
            }
        }

        // > Decimal256(S)
        if let Some(s) = s.strip_prefix("Decimal256(") {
            if let Some(s) = s.strip_suffix(')') {
                let s = s.trim().parse::<u8>()?;
                return Ok(Type::Decimal256(s));
            } else {
                return Err(Error::new("invalid Decimal256 type"));
            }
        }

        // > FixedString(N)
        if let Some(s) = s.strip_prefix("FixedString(") {
            if let Some(s) = s.strip_suffix(')') {
                let n = s.trim().parse::<u8>()?;
                return Ok(Type::FixedString(n));
            } else {
                return Err(Error::new("invalid FixedString type"));
            }
        }

        // > DateTime64(P)
        if let Some(s) = s.strip_prefix("DateTime64(") {
            if let Some(s) = s.strip_suffix(')') {
                let p = s.trim().parse::<u8>()?;
                return Ok(Type::DateTime64(p));
            } else {
                return Err(Error::new("invalid DateTime64 type"));
            }
        }

        // > Enum8(...)
        if let Some(s) = s.strip_prefix("Enum8(") {
            if let Some(s) = s.strip_suffix(')') {
                let mut map = BTreeMap::new();
                for variant in s.split(',') {
                    let parts = variant.trim().split('=').collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(Error::new("invalid Enum8 type"));
                    }
                    let name = parts[0]
                        .trim()
                        .trim_start_matches('\'')
                        .trim_end_matches('\'')
                        .to_string();
                    let index = parts[1].trim().parse::<i8>()?;
                    map.insert(name, index);
                }
                return Ok(Type::Enum8(map));
            } else {
                return Err(Error::new("invalid DateTime64 type"));
            }
        }

        // > Enum16(...)
        if let Some(s) = s.strip_prefix("Enum16(") {
            if let Some(s) = s.strip_suffix(')') {
                let mut map = BTreeMap::new();
                for variant in s.split(',') {
                    let parts = variant.trim().split('=').collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(Error::new("invalid Enum8 type"));
                    }
                    let name = parts[0]
                        .trim()
                        .trim_start_matches('\'')
                        .trim_end_matches('\'')
                        .to_string();
                    let index = parts[1].trim().parse::<i16>()?;
                    map.insert(name, index);
                }
                return Ok(Type::Enum16(map));
            } else {
                return Err(Error::new("invalid DateTime64 type"));
            }
        }

        // > Enum(...)
        if let Some(s) = s.strip_prefix("Enum(") {
            if let Some(s) = s.strip_suffix(')') {
                let mut map = BTreeMap::new();
                for variant in s.split(',') {
                    let parts = variant.trim().split('=').collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(Error::new("invalid Enum8 type"));
                    }
                    let name = parts[0]
                        .trim()
                        .trim_start_matches('\'')
                        .trim_end_matches('\'')
                        .to_string();
                    let index = parts[1].trim().parse::<i16>()?;
                    map.insert(name, index);
                }
                return Ok(Type::Enum16(map));
            } else {
                return Err(Error::new("invalid DateTime64 type"));
            }
        }

        // > Array
        if let Some(s) = s.strip_prefix("Array(") {
            if let Some(s) = s.strip_suffix(')') {
                let ty = s.trim().parse::<Type>()?;
                return Ok(Type::Array(Box::new(ty)));
            } else {
                return Err(Error::new("invalid Array type"));
            }
        }

        // > Tuple
        if let Some(s) = s.strip_prefix("Tuple(") {
            if let Some(s) = s.strip_suffix(')') {
                let mut types = vec![];
                for ty_str in s.split(',') {
                    let ty = ty_str.trim().parse::<Type>()?;
                    types.push(ty);
                }
                return Ok(Type::Tuple(types));
            } else {
                return Err(Error::new("invalid Tuple type"));
            }
        }

        // > Map
        if let Some(s) = s.strip_prefix("Map(") {
            if let Some(s) = s.strip_suffix(')') {
                let parts = s.split(',').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(Error::new("invalid Map type"));
                }
                let key_ty = parts[0].trim().parse::<Type>()?;
                let val_ty = parts[1].trim().parse::<Type>()?;
                return Ok(Type::Map(Box::new(key_ty), Box::new(val_ty)));
            } else {
                return Err(Error::new("invalid Map type"));
            }
        }

        // > Nested
        if let Some(s) = s.strip_prefix("Nested(") {
            if let Some(s) = s.strip_suffix(')') {
                let mut fields = vec![];
                for field_str in s.split(',').collect::<Vec<_>>() {
                    let parts = field_str.trim().split(' ').collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(Error::new("invalid Nested type"));
                    }
                    let name = parts[0].trim();
                    let ty = parts[1].trim().parse::<Type>()?;
                    fields.push((name.to_string(), ty));
                }

                return Ok(Type::Nested(fields));
            } else {
                return Err(Error::new("invalid Nested type"));
            }
        }

        // > Nullable(T)
        if let Some(s) = s.strip_prefix("Nullable(") {
            if let Some(s) = s.strip_suffix(')') {
                let ty = s.parse::<Type>()?;
                let ty = match ty {
                    Type::UInt8 => Type::NullableUInt8,
                    Type::UInt16 => Type::NullableUInt16,
                    Type::UInt32 => Type::NullableUInt32,
                    Type::UInt64 => Type::NullableUInt64,
                    Type::UInt128 => Type::NullableUInt128,
                    Type::UInt256 => Type::NullableUInt256,
                    Type::Int8 => Type::NullableInt8,
                    Type::Int16 => Type::NullableInt16,
                    Type::Int32 => Type::NullableInt32,
                    Type::Int64 => Type::NullableInt64,
                    Type::Int128 => Type::NullableInt128,
                    Type::Int256 => Type::NullableInt256,
                    Type::Float32 => Type::NullableFloat32,
                    Type::Float64 => Type::NullableFloat64,
                    Type::Decimal(p, s) => Type::NullableDecimal(p, s),
                    Type::Decimal32(s) => Type::NullableDecimal32(s),
                    Type::Decimal64(s) => Type::NullableDecimal64(s),
                    Type::Decimal128(s) => Type::NullableDecimal128(s),
                    Type::Decimal256(s) => Type::NullableDecimal256(s),
                    Type::Bool => Type::NullableBool,
                    Type::String => Type::NullableString,
                    Type::FixedString(n) => Type::NullableFixedString(n),
                    Type::UUID => Type::NullableUUID,
                    Type::Date => Type::NullableDate,
                    Type::Date32 => Type::NullableDate32,
                    Type::DateTime => Type::NullableDateTime,
                    Type::DateTime64(p) => Type::NullableDateTime64(p),
                    Type::Enum8(keys) => Type::NullableEnum8(keys),
                    Type::Enum16(keys) => Type::NullableEnum16(keys),
                    _ => return Err(Error::new("invalid Nullable type")),
                };
                return Ok(ty);
            } else {
                return Err(Error::new("invalid Nullable type"));
            }
        }

        Err(Error::new(
            format!("'{s}' is not a valid Clickhouse type").as_str(),
        ))
    }
}

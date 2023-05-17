//! Formats

mod rowbin;
mod tab;

pub use rowbin::*;
pub use tab::*;

use crate::{
    error::Error,
    value::{Type, Value},
};

use super::QueryData;

/// Clickhouse formatter
///
/// A formatter serializes and deserializes
pub trait Formatter {
    /// Serializes a [Value]
    fn serialize_value(&self, value: Value) -> Vec<u8>;

    /// Serializes a [QueryData]
    fn serialize_query_data(&self, data: QueryData) -> Result<Vec<u8>, Error>;

    /// Deserializes bytes to a [Value]
    fn deserialize_value(&self, bytes: &[u8], ty: Type) -> Result<Value, Error>;

    /// Deserializes bytes to a [QueryData]
    fn deserialize_query_data(
        &self,
        bytes: &[u8],
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<QueryData, Error>;
}

impl Value {
    /// Serializes a [Value] to bytes
    pub fn to_bytes(self, format: Format) -> Vec<u8> {
        let formatter = format.formatter();
        formatter.serialize_value(self)
    }

    /// Deserializes a buffer to a [Value]
    pub fn from_bytes(bytes: &[u8], format: Format, ty: Type) -> Result<Value, Error> {
        let formatter = format.formatter();
        let value = formatter.deserialize_value(bytes, ty)?;
        Ok(value)
    }
}

impl QueryData {
    /// Converts to bytes
    pub fn to_bytes(self, format: Format) -> Result<Vec<u8>, Error> {
        let formatter = format.formatter();
        formatter.serialize_query_data(self)
    }

    /// Parses from bytes
    pub fn from_bytes(
        bytes: &[u8],
        format: Format,
        mapping: Option<&[(&str, Type)]>,
    ) -> Result<Self, Error> {
        let formatter = format.formatter();
        let table = formatter.deserialize_query_data(bytes, mapping)?;
        Ok(table)
    }
}

/// Query format
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum Format {
    TabSep,
    TabSepdRaw,
    TabSepWithNames,
    TabSepWithNamesAndTypes,
    TabSepRawWithNames,
    TabSepRawWithNamesAndTypes,
    Template,
    TemplateIgnoreSpaces,
    CSV,
    CSVWithNames,
    CSVWithNamesAndTypes,
    CustomSeparated,
    CustomSeparatedWithNames,
    CustomSeparatedWithNamesAndTypes,
    SQLInsert,
    Values,
    Vertical,
    JSON,
    JSONAsString,
    JSONStrings,
    JSONColumns,
    JSONColumnsWithMetadata,
    JSONCompact,
    JSONCompactStrings,
    JSONCompactColumns,
    JSONEachRow,
    PrettyJSONEachRow,
    JSONEachRowWithProgress,
    JSONStringsEachRow,
    JSONStringsEachRowWithProgress,
    JSONCompactEachRow,
    JSONCompactEachRowWithNames,
    JSONCompactEachRowWithNamesAndTypes,
    JSONCompactStringsEachRow,
    JSONCompactStringsEachRowWithNames,
    JSONCompactStringsEachRowWithNamesAndTypes,
    JSONObjectEachRow,
    BSONEachRow,
    TSKV,
    Pretty,
    PrettyNoEscapes,
    PrettyMonoBlock,
    PrettyNoEscapesMonoBlock,
    PrettyCompact,
    PrettyCompactNoEscapes,
    PrettyCompactMonoBlock,
    PrettyCompactNoEscapesMonoBlock,
    PrettySpace,
    PrettySpaceNoEscapes,
    PrettySpaceMonoBlock,
    PrettySpaceNoEscapesMonoBlock,
    Prometheus,
    Protobuf,
    ProtobufSingle,
    Avro,
    AvroConfluent,
    Parquet,
    ParquetMetadata,
    Arrow,
    ArrowStream,
    ORC,
    One,
    RowBinary,
    RowBinaryWithNames,
    RowBinaryWithNamesAndTypes,
    RowBinaryWithDefaults,
    Native,
    XML,
    CapnProto,
    LineAsString,
    RawBLOB,
    MsgPack,
    MySQLDump,
    Markdown,
}

impl Format {
    /// Returns the formatter
    pub fn formatter(&self) -> Box<dyn Formatter> {
        match self {
            Self::TabSep => Box::new(TsvFormatter::new()),
            Self::TabSepWithNames => Box::new(TsvFormatter::with_names()),
            Self::TabSepWithNamesAndTypes => Box::new(TsvFormatter::with_names_and_types()),
            Self::TabSepdRaw => Box::new(TsvFormatter::raw()),
            Self::TabSepRawWithNames => Box::new(TsvFormatter::raw_with_names()),
            Self::TabSepRawWithNamesAndTypes => Box::new(TsvFormatter::raw_with_names_and_types()),
            Self::RowBinary => Box::new(RowBinFormatter::new()),
            Self::RowBinaryWithNames => Box::new(RowBinFormatter::with_names()),
            Self::RowBinaryWithNamesAndTypes => Box::new(RowBinFormatter::with_names_and_types()),
            _ => unimplemented!(),
        }
    }
}

// NB: the string is the Clickhouse format name
impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let format = match self {
            Format::TabSep => "TSV",
            Format::TabSepdRaw => "TSVRaw",
            Format::TabSepWithNames => "TSVWithNames",
            Format::TabSepWithNamesAndTypes => "TSVWithNamesAndTypes",
            Format::TabSepRawWithNames => "TSVRawWithNames",
            Format::TabSepRawWithNamesAndTypes => "TSVRawWithNamesAndTypes",
            Format::Template => "Template",
            Format::TemplateIgnoreSpaces => "TemplateIgnoreSpaces",
            Format::CSV => "CSV",
            Format::CSVWithNames => "CSVWithNames",
            Format::CSVWithNamesAndTypes => "CSVWithNamesAndTypes",
            Format::CustomSeparated => "CustomSeparated",
            Format::CustomSeparatedWithNames => "CustomSeparatedWithNames",
            Format::CustomSeparatedWithNamesAndTypes => "CustomSeparatedWithNamesAndTypes",
            Format::SQLInsert => "SQLInsert",
            Format::Values => "Values",
            Format::Vertical => "Vertical",
            Format::JSON => "JSON",
            Format::JSONAsString => "JSONAsString",
            Format::JSONStrings => "JSONStrings",
            Format::JSONColumns => "JSONColumns",
            Format::JSONColumnsWithMetadata => "JSONColumnsWithMetadata",
            Format::JSONCompact => "JSONCompact",
            Format::JSONCompactStrings => "JSONCompactStrings",
            Format::JSONCompactColumns => "JSONCompactColumns",
            Format::JSONEachRow => "JSONEachRow",
            Format::PrettyJSONEachRow => "PrettyJSONEachRow",
            Format::JSONEachRowWithProgress => "JSONEachRowWithProgress",
            Format::JSONStringsEachRow => "JSONStringsEachRow",
            Format::JSONStringsEachRowWithProgress => "JSONStringsEachRowWithProgress",
            Format::JSONCompactEachRow => "JSONCompactEachRow",
            Format::JSONCompactEachRowWithNames => "JSONCompactEachRowWithNames",
            Format::JSONCompactEachRowWithNamesAndTypes => "JSONCompactEachRowWithNamesAndTypes",
            Format::JSONCompactStringsEachRow => "JSONCompactStringsEachRow",
            Format::JSONCompactStringsEachRowWithNames => "JSONCompactStringsEachRowWithNames",
            Format::JSONCompactStringsEachRowWithNamesAndTypes => {
                "JSONCompactStringsEachRowWithNamesAndTypes"
            }
            Format::JSONObjectEachRow => "JSONObjectEachRow",
            Format::BSONEachRow => "BSONEachRow",
            Format::TSKV => "TSKV",
            Format::Pretty => "Pretty",
            Format::PrettyNoEscapes => "PrettyNoEscapes",
            Format::PrettyMonoBlock => "PrettyMonoBlock",
            Format::PrettyNoEscapesMonoBlock => "PrettyNoEscapesMonoBlock",
            Format::PrettyCompact => "PrettyCompact",
            Format::PrettyCompactNoEscapes => "PrettyCompactNoEscapes",
            Format::PrettyCompactMonoBlock => "PrettyCompactMonoBlock",
            Format::PrettyCompactNoEscapesMonoBlock => "PrettyCompactNoEscapesMonoBlock",
            Format::PrettySpace => "PrettySpace",
            Format::PrettySpaceNoEscapes => "PrettySpaceNoEscapes",
            Format::PrettySpaceMonoBlock => "PrettySpaceMonoBlock",
            Format::PrettySpaceNoEscapesMonoBlock => "PrettySpaceNoEscapesMonoBlock",
            Format::Prometheus => "Prometheus",
            Format::Protobuf => "Protobuf",
            Format::ProtobufSingle => "ProtobufSingle",
            Format::Avro => "Avro",
            Format::AvroConfluent => "AvroConfluent",
            Format::Parquet => "Parquet",
            Format::ParquetMetadata => "ParquetMetadata",
            Format::Arrow => "Arrow",
            Format::ArrowStream => "ArrowStream",
            Format::ORC => "ORC",
            Format::One => "One",
            Format::RowBinary => "RowBinary",
            Format::RowBinaryWithNames => "RowBinaryWithNames",
            Format::RowBinaryWithNamesAndTypes => "RowBinaryWithNamesAndTypes",
            Format::RowBinaryWithDefaults => "RowBinaryWithDefaults",
            Format::Native => "Native",
            Format::XML => "XML",
            Format::CapnProto => "CapnProto",
            Format::LineAsString => "LineAsString",
            Format::RawBLOB => "RawBLOB",
            Format::MsgPack => "MsgPack",
            Format::MySQLDump => "MySQLDump",
            Format::Markdown => "Markdown",
        };
        write!(f, "{}", format)
    }
}

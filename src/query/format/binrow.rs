//! RowBinary format

use std::io::{Read, Write};

use crate::{error::Error, schema::TableSchema};

/// Formatter for the `RowBin` format
pub struct RowBinFormatter;

/// Extension trait for conversion from/to row binary types
pub trait RowBinTypeExt {
    /// Converts to its bytes representation
    fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error>;
}

/// Formatter for the format `RowBinaryWithNames`
///
/// This format is a concatenation of binary values.
///
/// It contains:
/// - HEADER: LEB128-encoded number of columns (N)
/// - HEADER: N strings of column names
/// - VALUES:
///     - strings are unsigned LEB128
#[derive(Debug)]
pub struct RowBinaryWithNamesFormatter<'a> {
    /// Schema
    schema: &'a TableSchema,
}

impl<'a> RowBinaryWithNamesFormatter<'a> {
    /// Creates a new formatter
    pub fn new(schema: &'a TableSchema) -> Self {
        Self { schema }
    }
}

impl<'a> RowBinaryWithNamesFormatter<'a> {
    /// Formats to the `RowBinaryWithNames` format
    pub fn format(
        &self,
        names: &[&str; 8],
        rows: &[&[Box<dyn RowBinTypeExt>]],
    ) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];

        // write columns
        let n_cols = names
            .len()
            .try_into()
            .expect("usize cannot be converted to u64");
        leb128::write::unsigned(&mut buf, n_cols)?;

        for name in names {
            leb128::write::unsigned(
                &mut buf,
                name.len().try_into().expect("cannot convert usize to u64"),
            )?;
            buf.write_all(name.as_bytes())?;
        }

        // write values
        for row in rows {
            for value in row.iter() {
                buf.write_all(&value.to_rowbin_bytes()?)?;
            }
        }

        Ok(buf)
    }

    /// Parses a query result
    pub fn parse(&self, mut bytes: &[u8]) -> Result<(), Error> {
        // parse column names
        let n_cols = leb128::read::unsigned(&mut bytes)?;
        eprintln!("n_cols: {n_cols}");

        let mut cols = vec![];
        let mut i = 0;
        loop {
            let str_len = leb128::read::unsigned(&mut bytes)?;
            let mut buf = vec![0u8; str_len as usize];
            bytes.read_exact(&mut buf)?;
            let col_name = String::from_utf8(buf)?;
            eprintln!("col_name: {col_name}");

            cols.push(col_name);

            i += 1;
            if i == n_cols {
                break;
            }
        }

        // parse values
        let str_len = leb128::read::unsigned(&mut bytes)?;
        eprintln!("str_len: {str_len}");

        let str_len = leb128::read::unsigned(&mut bytes)?;
        eprintln!("str_len: {str_len}");

        let str_len = leb128::read::unsigned(&mut bytes)?;
        eprintln!("str_len: {str_len}");
        Ok(())
    }
}

// -- TYPE IMPLEMENTATIONS --

/// Implements the RowBinTypeExt for integers
macro_rules! impl_rowbin_type_ext {
    ($TY:ty) => {
        impl RowBinTypeExt for $TY {
            fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
                Ok(self.to_le_bytes().to_vec())
            }
        }
    };
}

impl_rowbin_type_ext!(u8);
impl_rowbin_type_ext!(u16);
impl_rowbin_type_ext!(u32);
impl_rowbin_type_ext!(u64);
impl_rowbin_type_ext!(u128);
impl_rowbin_type_ext!(usize);
impl_rowbin_type_ext!(i8);
impl_rowbin_type_ext!(i16);
impl_rowbin_type_ext!(i32);
impl_rowbin_type_ext!(i64);
impl_rowbin_type_ext!(i128);
impl_rowbin_type_ext!(isize);
impl_rowbin_type_ext!(f32); // check
impl_rowbin_type_ext!(f64); // check

impl RowBinTypeExt for String {
    fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
        let bytes = self.as_bytes().to_vec();
        let n = bytes.len();
        let mut buf = vec![];
        leb128::write::unsigned(&mut buf, n.try_into()?)?;
        buf.write_all(&bytes)?;
        Ok(buf)
    }
}

impl<'a> RowBinTypeExt for &'a str {
    fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
        self.to_string().to_rowbin_bytes()
    }
}

// Nullable
impl<T> RowBinTypeExt for Option<T>
where
    T: RowBinTypeExt,
{
    fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
        match self {
            Some(t) => {
                // a Nullbale non NULL value starts with 0
                let mut bytes = vec![0];
                bytes.append(&mut t.to_rowbin_bytes()?);
                Ok(bytes)
            }
            None => {
                // NB: a nullable value is just 1
                Ok([1].to_vec())
            }
        }
    }
}

/// Extension for the `time` crate
#[cfg(feature = "time")]
mod time {
    use super::*;

    use ::time::{Date, Month, OffsetDateTime};

    // Date => u16 = number of days since 1970-01-01 [1970-01-01, 2149-06-06]
    // Date32 => u32 = days since 1970-01-01
    impl RowBinTypeExt for Date {
        fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
            let days = self.to_julian_day()
                - Date::from_calendar_date(1970, Month::January, 1)
                    .unwrap()
                    .to_julian_day();
            let days: u16 = days.try_into()?;
            days.to_rowbin_bytes()
        }
    }

    // DateTime => u32, seconds since 1970-01-01 [1970-01-01 00:00:00, 2106-02-07 06:28:15]
    // DateTime64(precision) => idem with subseconds
    impl RowBinTypeExt for OffsetDateTime {
        fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
            let unix: u32 = self.unix_timestamp().try_into()?;
            Ok(unix.to_le_bytes().to_vec())
        }
    }
}

/// Extension for the `uuid` crate
#[cfg(feature = "uuid")]
mod uuid {
    use super::*;

    use ::uuid::Uuid;

    impl RowBinTypeExt for Uuid {
        fn to_rowbin_bytes(&self) -> Result<Vec<u8>, Error> {
            Ok(self.as_bytes().to_vec())
        }
    }
}

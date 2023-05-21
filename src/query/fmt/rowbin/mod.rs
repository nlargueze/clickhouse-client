//! RowBinary format

mod de;
mod ser;

#[cfg(test)]
mod tests;

pub use de::*;
pub use ser::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    error::Error,
    orm::{Type, Value},
};

use super::FmtDataTable;

impl FmtDataTable {
    /// Serializes with the RowBin formatter
    ///
    /// NB: We cannot use the standard desarialization, since the header is
    pub(crate) fn serialize_rowbin(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];
        let serializer = RowBinSerializer::new();

        let mut n_cols = 0;
        if let Some(names) = &self.names {
            n_cols = names.len();
            leb128::write::unsigned(&mut buf, n_cols as u64).unwrap();

            for name in names {
                let mut bytes = serializer.serialize_str(name)?;
                buf.append(&mut bytes);
            }

            if let Some(types) = &self.types {
                debug_assert_eq!(types.len(), n_cols);

                for ty in types {
                    let ty_str = ty.to_string();
                    let mut bytes = serializer.serialize_str(&ty_str)?;
                    buf.append(&mut bytes);
                }
            }
        }

        for row in &self.rows {
            // NB: we do a sanity check that the length of Vec matches
            #[cfg(debug_assertions)]
            {
                if n_cols == 0 {
                    n_cols = row.len();
                } else {
                    debug_assert_eq!(row.len(), n_cols);
                }
            }

            for value in row {
                let mut bytes = value.serialize(serializer)?;
                buf.append(&mut bytes);
            }
        }

        Ok(buf)
    }

    /// Deserializes with the RowBin formatter
    pub(crate) fn deserialize_rowbin(
        bytes: &[u8],
        has_names: bool,
        has_types: bool,
    ) -> Result<Self, Error> {
        let mut buf = bytes;
        let deserializer = RowBinDeserializer::new(buf);
        let mut table = FmtDataTable::default();

        if has_names || has_types {
            let n = leb128::read::unsigned(&mut buf).unwrap();

            let mut names = vec![];
            for i in 0..n {
                let name = String::deserialize(deserializer)?;
                names.push(name);
            }
            table.names = Some(names);

            if has_types {
                // NB: names must be defined if types are provied
                debug_assert!(has_names);

                let mut types = vec![];
                for i in 0..n {
                    let ty_str = String::deserialize(deserializer)?;
                    let ty = ty_str.parse::<Type>()?;
                    types.push(ty);
                }
                table.types = Some(types);
            }
        }

        let buf = deserializer.remaining();
        table.deserialize_rowbin_no_header(buf)?;
        Ok(table)
    }

    /// Deserializes with the RowBin formatter and no header
    ///
    /// The source bytes do not contain any header, just a concatenated stream of rows and values.
    /// That means that the types must have been defined on the table beforehand.
    pub(crate) fn deserialize_rowbin_no_header(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let buf = bytes;
        let deserializer = RowBinDeserializer::new(buf);
        let types = self.types.as_ref().ok_or_else(|| {
            Error::new("deserializing a RowBinary format requires types to have been parsed")
        })?;

        // values
        // loop on all rows until everything is read
        let mut rows = vec![];
        'l_rows: loop {
            // loop on each columns
            let mut cols = vec![];
            for ty in types {
                let value = Value::deserialize_type(deserializer, ty.clone()).unwrap();
                cols.push(value);
            }
            rows.push(cols);

            // break when there is nothing left to read.
            if deserializer.remaining().is_empty() {
                break 'l_rows;
            }
        }
        self.rows = rows;

        Ok(())
    }
}

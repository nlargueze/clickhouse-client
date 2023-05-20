//! RowBinary format

mod de;
mod ser;

use serde::{Deserialize, Deserializer, Serializer};

use crate::{error::Error, ty::Type};

#[cfg(test)]
mod tests;

/// RowBinary representation of a table of rows
#[derive(Debug)]
pub(crate) struct FmtDataTable<const N: usize> {
    /// Columns
    pub columns: Option<([String; N])>,
    /// Types
    pub types: Option<[Type; N]>,
    /// Rows
    pub rows: [Vec<u8>; N],
}

impl<const N: usize> FmtDataTable<N> {
    /// Serializes with the RowBin formatter
    pub(crate) fn serialize_rowbin(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];
        if self.columns.is_some() || self.types.is_some() {
            leb128::write::unsigned(&mut buf, N as u64).unwrap();
        }

        if let Some(columns) = &self.columns {
            for col in columns {
                let serializer = ser::RowBinSerializer::new();
                let mut bytes = serializer.serialize_str(col)?;
                buf.append(&mut bytes);
            }
        }

        if let Some(types) = &self.types {
            for ty in types {
                // let serializer = ser::RowBinSerializer::new();
                // // let mut bytes = serializer.serialize_str(ty)?;
                // buf.append(&mut bytes);
                todo!()
            }
        }

        Ok(buf)
    }

    /// Deserializes with the RowBin formatter
    pub(crate) fn deserialize_rowbin(bytes: &[u8], has_names: Option<bool>) -> Result<Self, Error> {
        let mut buf = bytes;

        let has_header = true;
        if has_header {
            let n = leb128::read::unsigned(&mut buf).unwrap();
            if n != N as u64 {
                // error
            }

            for i in 0..N {
                let deserializer = de::RowBinDeserializer::new(buf);
                // let col_name = deserializer.deserialize_str(visitor).unwrap();
                // => get a string +add to array
            }
        }

        let has_types = true;
        if has_types {
            let deserializer = de::RowBinDeserializer::new(buf);

            for i in 0..N {
                let deserializer = de::RowBinDeserializer::new(buf);
                // let col_name = deserializer.deserialize_str(visitor).unwrap();
                // => get a string +add to array
            }
        }

        // loop on all rows until everything is read
        'l_rows: loop {
            // loop on each columns
            'l_cols: for i in 0..N {
                // we need to know the column type to know how to deserialize it
                // Type enum match
                //
                let deserializer = de::RowBinDeserializer::new(buf);
                let i8 = i8::deserialize(deserializer).unwrap();
            }
        }

        todo!()
    }
}

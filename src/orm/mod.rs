//! ORM

mod query;

#[cfg(test)]
mod tests;

use crate::{
    error::Error,
    query::QueryData,
    schema::TableSchema,
    value::{Type, Value},
};

pub use query::*;

/// ORM prelude
pub mod prelude {
    pub use super::{ChRecord, Record};
    pub use crate::{
        error::Error,
        schema::TableSchema,
        value::{ChValue, Type, Value},
    };
    pub use clickhouse_client_macros::AsChRecord;
}

/// A DB record
#[derive(Debug, Clone)]
pub struct Record {
    /// Table name
    pub table: String,
    /// Fields
    pub fields: Vec<RecordField>,
}

/// A DB record field
#[derive(Debug, Clone)]
pub struct RecordField {
    /// ID
    pub id: String,
    /// Is primary key
    pub primary: bool,
    /// Type
    pub ty: Type,
    /// Value
    pub value: Value,
}

impl Record {
    /// Creates an empty record
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            fields: vec![],
        }
    }

    /// Adds a field
    pub fn add_field(&mut self, id: &str, primary: bool, ty: Type, value: Value) -> &mut Self {
        self.fields.push(RecordField {
            id: id.to_string(),
            primary,
            ty,
            value,
        });
        self
    }

    /// Adds a field
    pub fn field(mut self, id: &str, primary: bool, ty: Type, value: Value) -> Self {
        self.fields.push(RecordField {
            id: id.to_string(),
            primary,
            ty,
            value,
        });
        self
    }

    /// Returns the [TableSchema]
    pub fn schema(&self) -> TableSchema {
        let mut table = TableSchema::new(self.table.as_str());
        for field in &self.fields {
            table.add_column(field.id.as_str(), field.ty.clone(), field.primary);
        }
        table
    }

    /// Returns all the fields
    pub fn fields(&self) -> Vec<&RecordField> {
        self.fields.iter().collect()
    }

    /// Returns the primary fields
    pub fn primary_fields(&self) -> Vec<&RecordField> {
        self.fields.iter().filter(|f| f.primary).collect::<Vec<_>>()
    }

    /// Returns a record field
    pub fn get_field(&self, id: &str) -> Option<&RecordField> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Removes a record field and returns ir
    pub fn remove_field(&mut self, id: &str) -> Option<RecordField> {
        let i = match self.fields.iter().position(|f| f.id == id) {
            Some(i) => i,
            None => return None,
        };
        Some(self.fields.remove(i))
    }
}

/// A trait to convert from/to a Clickhouse [Record]
pub trait ChRecord: Sized {
    /// Returns the Clickhouse schema
    fn ch_schema() -> TableSchema;

    /// Converts to a [Record]
    fn into_ch_record(self) -> Record;

    /// Converts from a [Record]
    fn from_ch_record(record: Record) -> Result<Self, Error>;

    /// Converts records to a [QueryTable]
    fn to_query_data(records: Vec<Self>) -> QueryData {
        let schema = Self::ch_schema();
        let mut table = QueryData::from_schema(&schema);
        for record in records {
            let record = record.into_ch_record();
            let row = record
                .fields
                .into_iter()
                .map(|f| f.value)
                .collect::<Vec<_>>();
            table.add_row(row);
        }
        table
    }

    /// Parses multiple records from a [QueryTable]
    fn from_query_data(data: QueryData) -> Result<Vec<Self>, Error> {
        let schema = Self::ch_schema();
        let parts = data.into_parts();
        let col_names = parts
            .names
            .ok_or(Error::new("Missing column names to parse table"))?
            .into_iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>();

        let mut records = vec![];
        for row in parts.rows {
            let mut record = Record::new(&schema.name);
            for (i, value) in row.into_iter().enumerate() {
                let id = col_names.get(i).ok_or(Error::new("Invalid column"))?;
                let col_sch = schema.get_column_by_id(id).ok_or(Error::new(""))?;
                let primary = col_sch.primary;
                let ty = col_sch.ty.clone();
                record.add_field(id, primary, ty, value);
            }
            let record = Self::from_ch_record(record)?;
            records.push(record);
        }

        Ok(records)
    }
}

// // -- TEST --

// use prelude::*;
// use time::Date;
// use uuid::Uuid;

// struct TestRecord {
//     /// ID
//     id: Uuid,
//     /// Name
//     name: String,
//     /// Count
//     count: u8,
//     /// Date
//     date: Date,
//     /// Optional
//     count_opt: Option<u8>,
// }

// impl ChRecord for TestRecord {
//     fn ch_schema() -> TableSchema {
//         TableSchema::new("test_orm")
//             .column("id", <Uuid>::ch_type(), true)
//             .column("name", <String>::ch_type(), false)
//             .column("count", <u8>::ch_type(), false)
//             .column("date", <Date>::ch_type(), false)
//             .column("count_opt", <Option<u8>>::ch_type(), false)
//     }

//     fn into_ch_record(self) -> Record {
//         Record::new("test_orm")
//             .field("id", true, <Uuid>::ch_type(), self.id.into_ch_value())
//             .field(
//                 "name",
//                 false,
//                 <String>::ch_type(),
//                 self.name.into_ch_value(),
//             )
//             .field("count", false, <u8>::ch_type(), self.count.into_ch_value())
//             .field("date", false, <Date>::ch_type(), self.date.into_ch_value())
//             .field(
//                 "count_opt",
//                 false,
//                 <Option<u8>>::ch_type(),
//                 self.count_opt.into_ch_value(),
//             )
//     }

//     /// Parses from a Clickhouse record
//     fn from_ch_record(mut record: Record) -> Result<Self, Error> {
//         Ok(Self {
//             id: match record.remove_field("id") {
//                 Some(field) => <Uuid>::from_ch_value(field.value)?,
//                 None => return Err(Error::new("Missing field 'id'")),
//             },
//             name: match record.remove_field("name") {
//                 Some(field) => <String>::from_ch_value(field.value)?,
//                 None => return Err(Error::new("Missing field 'name'")),
//             },
//             count: match record.remove_field("count") {
//                 Some(field) => <u8>::from_ch_value(field.value)?,
//                 None => return Err(Error::new("Missing field 'count'")),
//             },
//             date: match record.remove_field("date") {
//                 Some(field) => <Date>::from_ch_value(field.value)?,
//                 None => return Err(Error::new("Missing field 'date'")),
//             },
//             count_opt: match record.remove_field("count_opt") {
//                 Some(field) => <Option<u8>>::from_ch_value(field.value)?,
//                 None => return Err(Error::new("Missing field 'date'")),
//             },
//         })
//     }
// }

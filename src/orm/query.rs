//! ORM queries
//!
//! ORM queries use the `BinaryRow` format to get data from and into the DB.

use crate::{error::Error, intf::Interface};

use super::{OrmExt, OrmQuery};

// DDL
impl<'a, T, U> OrmQuery<'a, T, U>
where
    T: Interface,
    U: OrmExt,
{
    /// Creates a record table
    #[tracing::instrument(skip(self), fields(table = U::db_schema().name))]
    pub async fn create_table(&self, engine: &str) -> Result<(), Error> {
        let schema = U::db_schema();
        self.client.ddl().create_table(schema, engine).await
    }
}

// OPS
impl<'a, T, U> OrmQuery<'a, T, U>
where
    T: Interface,
    U: OrmExt,
{
    // /// Inserts records
    // #[tracing::instrument(skip(self, records), fields(table = U::db_schema().name))]
    // pub async fn insert(&self, records: &[U]) -> Result<(), Error> {
    //     let schema = U::db_schema();

    //     let table = self
    //         .client
    //         .raw_query_opts()
    //         .db
    //         .map(|db| format!("{}.{}", db, schema.name))
    //         .unwrap_or(schema.name.clone());

    //     let formatter = RowBinFormatter::new_with_names_and_types();
    //     let mut datatable = QueryTable::default();
    //     for (i, record) in records.iter().enumerate() {
    //         if i == 0 {
    //             datatable.names = record.db_names();
    //             datatable.types = record.db_types();
    //         }
    //         datatable.rows.push(record.db_row());
    //     }

    //     // nothing to insert
    //     if datatable.nb_rows() == 0 {
    //         return Ok(());
    //     }
    //     let data = formatter.format_table(&datatable);

    //     let query = format!(
    //         "INSERT INTO {} FROM {} FORMAT RowBinaryWithNamesAndTypes",
    //         table, "01234",
    //     );
    //     let options = self.client.raw_query_opts();
    //     let _res_bytes = self.client.interface.send(&query, options).await?;
    //     Ok(())
    // }

    // /// Select records
    // #[tracing::instrument(skip(self), fields(table = U::db_schema().name))]
    // pub async fn select(&self, columns: &[&str]) -> Result<(), Error> {
    //     // todo!();
    //     let schema = U::db_schema();

    //     let table = self
    //         .client
    //         .raw_query_opts()
    //         .db
    //         .map(|db| format!("{}.{}", db, schema.name))
    //         .unwrap_or(schema.name.to_string());

    //     let cols = if columns.is_empty() {
    //         vec!["*"]
    //     } else {
    //         columns.to_vec()
    //     };

    //     let query = format!(
    //         "SELECT {} FROM {} FORMAT RowBinaryWithNames",
    //         cols.join(","),
    //         table
    //     );

    //     let options = self.client.raw_query_opts();
    //     let _bytes = self.client.interface.send(&query, options).await?;

    //     // let formatter = RowBinFormatter::new();
    //     // formatter.parse_table(bytes, types);
    //     // QueryTable::from_schema(&schema).fill_with(format);
    //     // formatter.parse(&bytes).unwrap();
    //     // eprintln!("{:X?}", bytes);

    //     // INSERT
    //     // let table = <value as OrmRecord>.to_query_table(formatter);

    //     // Ok(())
    //     todo!()
    // }
}

// impl<F> Client<F>
// where
//     F: Interface,
// {
//     /// Inserts 1 or several records
//     ///
//     /// # Arguments
//     ///
//     /// If no columns are passed, all columns are returned.
//     ///
//     /// # Returns
//     ///
//     /// In Clickhouse, there is no RETURNING statement, so nothing is returned.
//     #[tracing::instrument(skip_all, fields(records.len = records.len()))]
//     pub async fn insert<T>(&self, records: &[T]) -> Result<(), Error>
//     where
//         T: DbRecordExt,
//     {

//     }

//     /// Selects 1 or several records
//     ///
//     /// # Arguments
//     ///
//     /// - if cols is empty, all fields are retrieved
//     #[tracing::instrument(skip(self))]
//     pub async fn select<T>(&self, cols: &[&str], where_cond: Where) -> Result<Vec<T>, Error>
//     where
//         T: DbRecordExt + Default,
//     {
//         let schema = T::SCHEMA;
//         let table = if let Some(db) = &self.db {
//             format!("{}.{}", db, schema.name)
//         } else {
//             schema.name.to_string()
//         };

//         let cols = if cols.is_empty() {
//             "*".to_string()
//         } else {
//             cols.join(", ")
//         };

//         let query = format!("SELECT {cols} FROM {table}{where_cond} FORMAT TabSeparatedWithNames");

//         let res_bytes = self.send_query(query.into()).await?;
//         let res_str = String::from_utf8(res_bytes)?;
//         tracing::trace!(query_res = res_str, "SELECT OK");

//         // parse the DB results
//         let mut res_cols = vec![];
//         let mut res_values = vec![];
//         for (i, line) in res_str.lines().enumerate() {
//             if i == 0 {
//                 // NB: this works because column names are not enclosed with single quotes,
//                 // and there is no special characters
//                 res_cols = line.split('\t').collect();
//             } else {
//                 let mut map = HashMap::new();
//                 for (j, val) in line.split('\t').enumerate() {
//                     let col = *res_cols.get(j).expect("shouldn't happen");
//                     map.insert(col, val);
//                 }
//                 res_values.push(map);
//             }
//         }
//         // tracing::debug!(columns = ?res_cols, values = ?res_values, "results parsed");

//         // parse to object T
//         let mut records = vec![];
//         for map in res_values {
//             let record = T::from_db_values(map).map_err(|err| Error::new(err.as_str()))?;
//             records.push(record);
//         }

//         Ok(records)
//     }

//     /// Updates a record
//     ///
//     /// # Arguments
//     ///
//     /// If no columns are provided, all columns are updated
//     #[tracing::instrument(skip(self, record))]
//     pub async fn update<T>(&self, record: &T, cols: &[&str], where_cond: Where) -> Result<(), Error>
//     where
//         T: DbRecordExt,
//     {
//         let schema = T::SCHEMA;
//         let table = if let Some(db) = &self.db {
//             format!("{}.{}", db, schema.name)
//         } else {
//             schema.name.to_string()
//         };

//         let col_values = record
//             .db_values()
//             .iter()
//             .filter_map(|(c, v)| {
//                 if cols.is_empty() || cols.contains(c) {
//                     Some(format!("{} = {}", c, v))
//                 } else {
//                     None
//                 }
//             })
//             .collect::<Vec<_>>()
//             .join(", ");

//         let query = format!("ALTER TABLE {} UPDATE {}{}", table, col_values, where_cond);
//         let _res_bytes = self.send_query(query.into()).await?;
//         Ok(())
//     }

//     /// Deletes a record
//     #[tracing::instrument(skip(self))]
//     pub async fn delete<T>(&self, where_cond: Where) -> Result<(), Error>
//     where
//         T: DbRecordExt,
//     {
//         let schema = T::SCHEMA;
//         let table = if let Some(db) = &self.db {
//             format!("{}.{}", db, schema.name)
//         } else {
//             schema.name.to_string()
//         };

//         // TODO: the lightweight DELETE FROM should be used, but it requires special grants
//         let query = format!("ALTER TABLE {} DELETE {}", table, where_cond);
//         let _res_bytes = self.send_query(query.into()).await?;
//         Ok(())
//     }
// }

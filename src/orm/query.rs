//! ORM queries
//!
//! ORM queries use the `BinaryRow` format to get data from and into the DB.

use std::marker::PhantomData;

use crate::{
    error::Error,
    intf::Interface,
    query::{Format, Where},
    Client,
};

use super::ChRecord;

/// ORM query
pub struct OrmQuery<'a, T, U>
where
    T: Interface,
    U: ChRecord,
{
    /// Client
    client: &'a Client<T>,
    /// Record
    record: PhantomData<U>,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Instantiates a [OrmQuery] instance
    pub fn orm<U>(&self) -> OrmQuery<T, U>
    where
        U: ChRecord,
    {
        OrmQuery {
            client: self,
            record: PhantomData,
        }
    }
}

impl<'a, T, U> OrmQuery<'a, T, U>
where
    T: Interface,
    U: ChRecord,
{
    /// Creates the record table
    #[tracing::instrument(skip(self), fields(table = U::ch_schema().name))]
    pub async fn create_table(&self, engine: &str) -> Result<(), Error> {
        let schema = U::ch_schema();
        self.client.ddl().create_table(&schema, engine).await
    }

    /// Drops the record table
    #[tracing::instrument(skip(self), fields(table = U::ch_schema().name))]
    pub async fn drop_table(&self) -> Result<(), Error> {
        let schema = U::ch_schema();
        self.client.ddl().drop_table(&schema.name).await
    }

    /// Inserts records
    #[tracing::instrument(skip(self, records), fields(table = U::ch_schema().name))]
    pub async fn insert(&self, records: Vec<U>) -> Result<(), Error> {
        let schema = U::ch_schema();
        let table = U::to_query_data(records);
        let _res = self.client.crud().insert(&schema.name, table).await?;
        Ok(())
    }

    /// Selects 1 or several records
    ///
    /// # Arguments
    ///
    /// - if cols is empty, all fields are retrieved
    #[tracing::instrument(skip(self))]
    pub async fn select(&self, where_cond: Option<Where>) -> Result<Vec<U>, Error> {
        let schema = U::ch_schema();
        let table = self
            .client
            .crud()
            .format(Format::RowBinaryWithNamesAndTypes)
            .select(&schema.name, vec![], where_cond)
            .await?
            .into_table(None)?;
        U::from_query_data(table)
    }

    /// Updates a record
    ///
    /// # Arguments
    ///
    /// - columns: if columns are provided, only those columns are updated
    #[tracing::instrument(skip(self, record))]
    pub async fn update_one(&self, record: U, columns: Vec<&str>) -> Result<(), Error> {
        let schema = U::ch_schema();
        let record = record.into_ch_record();
        let primary_fields = record.primary_fields();

        let fields = record
            .fields
            .iter()
            .filter_map(|f| {
                // primary fields cannot be updated
                if f.primary {
                    return None;
                }
                if !columns.is_empty() && !columns.contains(&f.id.as_str()) {
                    return None;
                }
                Some((f.id.as_str(), f.value.clone()))
            })
            .collect::<Vec<_>>();

        let where_cond = Where::new(
            format!(
                "({})",
                primary_fields
                    .iter()
                    .map(|f| f.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .as_str(),
            "IN",
            format!(
                "(({}))",
                primary_fields
                    .iter()
                    .map(|f| { f.value.clone().to_sql_string() })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .as_str(),
        );

        let _res = self
            .client
            .crud()
            .update(&schema.name, fields, Some(where_cond))
            .await?;
        Ok(())
    }

    /// Delete records
    #[tracing::instrument(skip(self, records), fields(table = U::ch_schema().name))]
    pub async fn delete(&self, records: Vec<U>) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }

        let schema = U::ch_schema();
        let mut primary_keys = vec![];
        let mut primary_values = vec![];
        for (i, record) in records.into_iter().enumerate() {
            let record = record.into_ch_record();
            let primary_fields = record.primary_fields();
            if i == 0 {
                primary_keys.extend(
                    primary_fields
                        .iter()
                        .map(|f| f.id.to_string())
                        .collect::<Vec<_>>(),
                );
            }
            primary_values.push(
                primary_fields
                    .iter()
                    .map(|f| f.value.clone())
                    .collect::<Vec<_>>(),
            );
        }

        let where_cond = Where::new(
            format!("({})", primary_keys.join(", ")).as_str(),
            "IN",
            format!(
                "({})",
                primary_values
                    .into_iter()
                    .map(|values| {
                        format!(
                            "({})",
                            values
                                .iter()
                                .map(|v| v.clone().to_sql_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .as_str(),
        );

        let _res = self
            .client
            .crud()
            .delete(&schema.name, Some(where_cond))
            .await?;
        Ok(())
    }
}

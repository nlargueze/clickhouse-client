//! SQL formatting

/// Trait to convert any value to an SQL which can be passed to a query
pub trait ToSqlString {
    /// Converts to a DB string
    fn to_sql_string(&self) -> String;
}

impl ToSqlString for String {
    fn to_sql_string(&self) -> String {
        // NB: strings must be enclosed by '
        // at least, the characters `'` and `\` must be escaped with a leading `\`
        // cf. https://clickhouse.com/docs/en/sql-reference/syntax
        let s = self.clone();
        let s = s.replace('\\', r"\\"); // NB: this has to go first
        let s = s.replace('\'', r"\'");
        format!("'{s}'")
    }
}

impl<'a> ToSqlString for &'a str {
    fn to_sql_string(&self) -> String {
        self.to_string().to_sql_string()
    }
}

/// Implements a base type which implements `to_string()` and `FromStr`
macro_rules! impl_base_type {
    ($TY:ty, $DB_TY:tt) => {
        impl ToSqlString for $TY {
            fn to_sql_string(&self) -> String {
                self.to_string()
            }
        }
    };
}

impl_base_type!(u8, "UInt8");
impl_base_type!(u16, "UInt16");
impl_base_type!(u32, "UInt32");
impl_base_type!(u64, "UInt64");
impl_base_type!(u128, "UInt128");
impl_base_type!(i8, "Int8");
impl_base_type!(i16, "Int16");
impl_base_type!(i32, "Int32");
impl_base_type!(i64, "Int64");
impl_base_type!(i128, "Int128");
impl_base_type!(f32, "Float32");
impl_base_type!(f64, "Float64");
impl_base_type!(bool, "Boolean");

// Nullable
impl<T> ToSqlString for Option<T>
where
    T: ToSqlString,
{
    fn to_sql_string(&self) -> String {
        match self {
            Some(t) => t.to_sql_string(),
            None => r"\N".to_string(),
        }
    }
}

// Reference
impl<'a, T> ToSqlString for &'a T
where
    T: ToSqlString,
{
    fn to_sql_string(&self) -> String {
        (*self).to_sql_string()
    }
}

/// Extension for the `time` crate
#[cfg(feature = "time")]
mod time {
    use super::*;

    use ::time::{macros::format_description, Date, OffsetDateTime, UtcOffset};

    impl ToSqlString for Date {
        fn to_sql_string(&self) -> String {
            // NB: CLickhouse accept the format
            // - '2023-05-06'
            let format = format_description!("[year]-[month]-[day]");
            let date_str = self.format(format).expect("invalid datetime");
            date_str.to_sql_string()
        }
    }

    impl ToSqlString for OffsetDateTime {
        fn to_sql_string(&self) -> String {
            // NB: Clickhouse accept the format
            // - '2023-05-06 20:03:15'
            // - Unix timestamp
            //
            // NB2: Clickhouse accepts only UTC dates (the timezone is a column metadata)
            let date_utc = self.to_offset(UtcOffset::UTC);
            let format =
                format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
            let time_str = date_utc.format(format).expect("invalid datetime");
            time_str.to_sql_string()
        }
    }
}

/// Extension for the `uuid` crate
#[cfg(feature = "uuid")]
mod uuid {
    use super::*;

    use ::uuid::Uuid;

    impl ToSqlString for Uuid {
        fn to_sql_string(&self) -> String {
            self.to_string().to_sql_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use time::{Date, OffsetDateTime};

    #[tokio::test]
    async fn test_query_sql_int() {
        let client = crate::tests::init().await;
        client
            .query("SELECT * FROM tests WHERE uint8 = ??")
            .bind(1)
            .exec()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_sql_string() {
        let client = crate::tests::init().await;
        client
            .query("SELECT * FROM tests WHERE string = ??")
            .bind("abc")
            .exec()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_sql_date() {
        let client = crate::tests::init().await;
        let date = Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
        client
            .query("SELECT * FROM tests WHERE date = ??")
            .bind(date)
            .exec()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_sql_date32() {
        let client = crate::tests::init().await;
        let date = Date::from_calendar_date(1970, time::Month::January, 1).unwrap();
        client
            .query("SELECT * FROM tests WHERE date32 = ??")
            .bind(date)
            .exec()
            .await
            .unwrap();
    }

    #[tokio::test]
    // NB: OffsetDateTime requires DateTime64(9) to allow for nanosecs
    #[should_panic]
    async fn test_query_sql_datetime() {
        let client = crate::tests::init().await;
        let date: OffsetDateTime = OffsetDateTime::now_utc();
        client
            .query("SELECT * FROM tests WHERE datetime = ??")
            .bind(date)
            .exec()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_sql_datetime64() {
        let client = crate::tests::init().await;
        let date: OffsetDateTime = OffsetDateTime::now_utc();
        client
            .query("SELECT * FROM tests WHERE datetime64 = ??")
            .bind(date)
            .exec()
            .await
            .unwrap();
    }
}

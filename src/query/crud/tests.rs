//! Tests for CRUD queries

use ethnum::{I256, U256};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};
use time::{Date, OffsetDateTime};
use uuid::Uuid;

use crate::{
    query::{QueryData, Where},
    schema::TableSchema,
    value::{time::DateExt, Type, Value},
};

/// Creates a test table and test data
pub fn sample_table() -> (TableSchema, QueryData) {
    let schema = TableSchema::new("test_crud")
        .column("uuid", Type::UUID, true)
        .column("uint8", Type::UInt8, false)
        .column("uint256", Type::UInt256, false)
        .column("int8", Type::Int8, false)
        .column("int256", Type::Int256, false)
        .column("float32", Type::Float32, false)
        .column("bool", Type::Bool, false)
        .column("string", Type::String, false)
        .column("date", Type::Date32, false)
        .column("datetime", Type::DateTime64(9), false)
        .column(
            "enum8",
            Type::Enum8(BTreeMap::from([
                ("var1".to_string(), 0),
                ("var2".to_string(), 1),
            ])),
            false,
        )
        .column("array", Type::Array(Box::new(Type::UInt8)), false)
        .column("tuple", Type::Tuple(vec![Type::UInt8, Type::String]), false)
        .column(
            "map",
            Type::Map(Box::new(Type::String), Box::new(Type::UInt8)),
            false,
        );

    let table = QueryData::from_schema(&schema).row(vec![
        Uuid::from_str("63712f62-a87a-4d0f-9673-a17380428dc4")
            .unwrap()
            .into(),
        1_u8.into(),
        U256::from_words(1, 1).into(),
        (-1_i8).into(),
        I256::from_words(-1, 1).into(),
        1.123_f32.into(),
        true.into(),
        "hello world".into(),
        Date::from_unix_days(10).unwrap().into(),
        OffsetDateTime::now_utc().into(),
        Value::Enum8(0),
        vec![1_u8, 2, 3].into(),
        (1_u8, "hey".to_string()).into(),
        HashMap::from([("key".to_string(), 1_u8)]).into(),
    ]);

    (schema, table)
}

// client.ddl().drop_table(&schema.name).await.unwrap();
// client
//     .ddl()
//     .create_table(&schema, "MergeTree()")
//     .await
//     .unwrap();
// client.crud().insert(&schema.name, table).await.unwrap();

#[tokio::test]
async fn query_crud() {
    let client = crate::tests::init().await;
    let (schema, mut table) = sample_table();

    client.ddl().drop_table(&schema.name).await.unwrap();
    client
        .ddl()
        .create_table(&schema, "MergeTree()")
        .await
        .unwrap();

    client
        .crud()
        .insert(&schema.name, table.clone())
        .await
        .unwrap();

    let uuid = Uuid::new_v4();
    table.get_rows_mut()[0][0] = uuid.into();
    client
        .crud()
        .update(
            &schema.name,
            vec![("uint8", 10.into())],
            Some(Where::with_val("uuid", "==", uuid)),
        )
        .await
        .unwrap();

    let table = client
        .crud()
        .select(&schema.name, vec![], None)
        .await
        .unwrap()
        .into_table(table.get_names_and_types().as_deref())
        .unwrap();
    eprintln!("{table}");
    assert_eq!(table.n_rows(), 1);

    client
        .crud()
        .delete(&schema.name, Some(Where::with_val("uuid", "==", uuid)))
        .await
        .unwrap();
}

//! Tests for query execution

use uuid::Uuid;

use crate::{query::Format, schema::TableSchema, value::Type};

/// A simple table schema
fn test_schema() -> TableSchema {
    TableSchema::new("test_exec")
        .column("id", Type::UUID, true)
        .column("is_valid", Type::Bool, false)
        .column("name", Type::String, false)
}

#[tokio::test]
async fn query_exec() {
    let client = crate::tests::init().await;
    let schema = test_schema();

    client.ddl().drop_table("test_exec").await.unwrap();
    client
        .ddl()
        .create_table(&schema, "MergeTree()")
        .await
        .unwrap();

    let _res = client
        .query("INSERT INTO [??] ([??]) VALUES ([??]);")
        .bind_str(&schema.name)
        .bind_str_list(vec!["id", "is_valid", "name"])
        .bind_val_list(vec![Uuid::new_v4().into(), false.into(), "name".into()])
        .exec()
        .await
        .unwrap();

    let res = client
        .query("SELECT * FROM [??]")
        .bind_str(&schema.name)
        .format(Format::TabSepRawWithNamesAndTypes)
        .exec()
        .await
        .unwrap();
    let table = res.into_table(None).unwrap();
    eprintln!("{table}");
}

//! Tests

// use super::prelude::*;

// /// Test record
// #[derive(Debug, Orm)]
// #[db(table = "records")]
// struct TestRecord {
//     /// ID
//     #[db(primary_key)]
//     id: u8,
//     /// Name
//     name: String,
// }

// impl TestRecord {
//     fn sample() -> Self {
//         TestRecord {
//             id: 1,
//             name: "john".into(),
//         }
//     }
// }

// #[tokio::test]
// async fn test_orm_select() {
//     let client = init().await;
//     let res = client.orm::<TestRecord>().select(&[]).await.unwrap();
// }

// /// Test record
// #[derive(Debug, DbRecord, Clone)]
// #[db(table = "records")]
// struct TestRecord {
//     #[db(primary)]
//     id: u32,
//     name: String,
//     timestamp: OffsetDateTime,
//     metric: f32,
//     null_int: Option<u8>,
//     array: Vec<String>,
// }

// impl Default for TestRecord {
//     fn default() -> Self {
//         Self {
//             id: Default::default(),
//             name: Default::default(),
//             timestamp: OffsetDateTime::UNIX_EPOCH,
//             metric: Default::default(),
//             null_int: Default::default(),
//             array: Vec::default(),
//         }
//     }
// }

// impl TestRecord {
//     fn sample() -> Self {
//         Self {
//             id: 1,
//             name: "name".to_string(),
//             timestamp: OffsetDateTime::now_utc(),
//             metric: 1.2,
//             null_int: None,
//             array: vec![
//                 "abcd".to_string(),
//                 "with_antislash\\".to_string(),
//                 "with_quote\'".to_string(),
//                 "with_\t_tab".to_string(),
//                 "with,comma".to_string(),
//             ],
//         }
//     }
// }

// static INIT: OnceCell<Client> = OnceCell::const_new();

// // #[tracing::instrument]
// async fn init() -> &'static Client {
//     INIT.get_or_init(|| async {
//         crate::tests::init_tracer();
//         let client = Client::new("http://localhost:8123").database("test");
//         let db_schema = DbSchema::new().table(TestRecord::db_schema());
//         client.create_db("test").await.unwrap();
//         for table_schema in db_schema.tables {
//             client
//                 .create_table(&table_schema, "MergeTree()")
//                 .await
//                 .unwrap();
//         }
//         client
//     })
//     .await
// }

// #[tokio::test]
// #[tracing::instrument]
// async fn test_orm_derive() {
//     init().await;

//     let record = TestRecord::sample();
//     let row_values = record.db_values();
//     tracing::info!(?row_values, "test_derive OK");
// }

// #[tokio::test]
// #[tracing::instrument]
// async fn test_orm_insert_select_update_delete() {
//     let client = init().await;

//     let record = TestRecord::sample();
//     match client.insert(&[record.clone()]).await {
//         Ok(_ok) => {
//             tracing::info!("insert OK");
//         }
//         Err(err) => {
//             tracing::error!(%err, "insert ERROR");
//             panic!("{err}")
//         }
//     }

//     let records = match client
//         .select::<TestRecord>(&[], Where::new("id", "=", 1))
//         .await
//     {
//         Ok(records) => {
//             tracing::info!("select OK");
//             records
//         }
//         Err(err) => {
//             tracing::error!(%err, "select ERROR");
//             panic!("{err}")
//         }
//     };

//     let record = records.into_iter().find(|r| r.id == 1).unwrap();
//     assert_eq!(record.id, 1);
//     assert_eq!(record.metric, 1.2);
//     assert_eq!(record.null_int, None);
//     assert_eq!(record.array.get(0).unwrap(), "abcd");
//     assert_eq!(record.array.get(1).unwrap(), "with_antislash\\");
//     assert_eq!(record.array.get(2).unwrap(), "with_quote\'");
//     assert_eq!(record.array.get(3).unwrap(), "with,comma");
//     assert_eq!(record.array.get(4).unwrap(), "with_\t_tab");

//     let mut updated_record = record;
//     updated_record.name = "update name".to_string();
//     match client
//         .update::<TestRecord>(
//             &updated_record,
//             &["name"],
//             Where::new("id", "=", updated_record.id),
//         )
//         .await
//     {
//         Ok(_ok) => {
//             tracing::info!("update OK");
//         }
//         Err(err) => {
//             tracing::error!(%err, "update ERROR");
//             panic!("{err}")
//         }
//     }

//     match client.delete::<TestRecord>(Where::new("id", "=", 1)).await {
//         Ok(_ok) => {
//             tracing::info!("delete OK");
//         }
//         Err(err) => {
//             tracing::error!(%err, "delete ERROR");
//             panic!("{err}")
//         }
//     }
// }

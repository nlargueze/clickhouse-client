//! HTTP tests

#[tokio::test]
async fn it_pings() {
    let client = crate::tests::init().await;
    assert!(client.ping().await);
}

#[tokio::test]
#[tracing::instrument]
async fn it_select_1() {
    let client = crate::tests::init().await;
    match client.query("SELECT 1").exec().await {
        Ok(res) => {
            // let res_body_str = String::from_utf8(res).unwrap();
            // tracing::info!(res_body_str, "test_http_raw_query OK");
        }
        Err(err) => {
            tracing::error!(%err, "test_http_raw_query ERROR");
            panic!("{err}")
        }
    }
}

//! Tests

use crate::{Client, HttpClient};

use std::sync::Once;
use tokio::sync::OnceCell;
use tracing_ext::sub::PrettyConsoleLayer;
use tracing_subscriber::{prelude::*, EnvFilter};

static INIT_TRACER: Once = Once::new();

static INIT_DB: OnceCell<()> = OnceCell::const_new();

/// Initializes a client (and a tracer, and a test table)
pub(crate) async fn init() -> HttpClient {
    INIT_TRACER.call_once(|| {
        let layer_pretty_stdout = PrettyConsoleLayer::default()
            .wrapped(true)
            .oneline(false)
            .events_only(false)
            .show_time(false)
            .show_target(true)
            .show_file_info(true)
            .show_span_info(true)
            .indent(6);
        let filter_layer = EnvFilter::from_default_env();

        tracing_subscriber::registry()
            .with(layer_pretty_stdout)
            .with(filter_layer)
            .init();
    });

    INIT_DB
        .get_or_init(|| async {
            let client = Client::default().database("tests");
            client.ddl().create_db("tests").await.unwrap();
        })
        .await;

    Client::default().database("tests")
}

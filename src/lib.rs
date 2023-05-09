//! Clickhouse client
//!
//! This crate provides a Clickhouse client.
//!
//! It relies on `hyper` for HTTP requests, `rustls` for TLS, and indirectly the tokio runtime.

pub mod error;
pub mod http;
pub mod schema;

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use tracing_ext::sub::PrettyConsoleLayer;
    use tracing_subscriber::{prelude::*, EnvFilter};

    static INIT: Once = Once::new();

    /// Initializes a tracer for unit tests
    pub fn init_test_tracer() {
        INIT.call_once(|| {
            let layer_pretty_stdout = PrettyConsoleLayer::default()
                .wrapped(true)
                .oneline(false)
                .events_only(false)
                .show_time(false)
                .show_target(true)
                .show_span_info(true)
                .indent(6);
            let filter_layer = EnvFilter::from_default_env();

            tracing_subscriber::registry()
                .with(layer_pretty_stdout)
                .with(filter_layer)
                .init();
        });
    }
}

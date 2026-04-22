//! Shared operational foundations reused across Legato binaries.

mod config;
mod error;
mod runtime;
mod telemetry;

pub use config::load_config;
pub use error::FoundationError;
pub use runtime::{ShutdownController, ShutdownToken};
pub use telemetry::{
    CommonProcessConfig, MetricKind, MetricSample, MetricsConfig, MetricsRegistry,
    ProcessTelemetry, TracingConfig, init_tracing, metric_name,
};

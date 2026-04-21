//! Shared tracing and metrics bootstrap conventions.

use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::FoundationError;

/// Tracing output configuration shared across Legato binaries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TracingConfig {
    /// Whether structured JSON output should be emitted.
    pub json: bool,
    /// Default filter expression when `RUST_LOG` is unset.
    pub level: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            json: false,
            level: String::from("info"),
        }
    }
}

/// Metrics naming and bind conventions shared across binaries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricsConfig {
    /// Optional bind address for metrics exposition when a process serves metrics directly.
    pub bind_address: Option<String>,
    /// Prefix prepended to exported metric names.
    pub prefix: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            bind_address: None,
            prefix: String::from("legato"),
        }
    }
}

/// Common operational config embedded inside binary-specific process config.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommonProcessConfig {
    /// Tracing output configuration.
    #[serde(default)]
    pub tracing: TracingConfig,
    /// Metrics naming configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

/// Initializes process-global tracing using shared formatting rules.
pub fn init_tracing(
    service_name: &str,
    tracing_config: &TracingConfig,
) -> Result<(), FoundationError> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(tracing_config.level.as_str()))?;

    let formatting = fmt::layer().with_target(true).with_thread_ids(true);

    if tracing_config.json {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                formatting
                    .json()
                    .with_current_span(false)
                    .flatten_event(true),
            )
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(formatting.compact())
            .try_init()?;
    }

    tracing::info!(service = service_name, "tracing initialized");
    Ok(())
}

/// Builds a namespaced metric name from a configured prefix and suffix.
#[must_use]
pub fn metric_name(metrics: &MetricsConfig, suffix: &str) -> String {
    format!("{}_{}", metrics.prefix, suffix)
}

#[cfg(test)]
mod tests {
    use super::{MetricsConfig, metric_name};

    #[test]
    fn metric_names_use_shared_prefix_convention() {
        let metrics = MetricsConfig {
            bind_address: None,
            prefix: String::from("legato_server"),
        };

        assert_eq!(
            metric_name(&metrics, "cache_hit_total"),
            "legato_server_cache_hit_total"
        );
    }
}

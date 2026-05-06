//! Shared tracing and metrics bootstrap conventions.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{FoundationError, ShutdownToken};

/// Tracing output configuration shared across Legato binaries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Whether structured JSON output should be emitted.
    pub json: bool,
    /// Default filter expression when `RUST_LOG` is unset.
    pub level: String,
    /// Optional directory for an append-only service log file.
    pub log_dir: Option<String>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            json: false,
            level: String::from("info"),
            log_dir: None,
        }
    }
}

/// Keeps any background tracing writer alive for the lifetime of the process.
#[derive(Debug)]
pub struct TracingGuard {
    _file_guard: Option<WorkerGuard>,
}

#[derive(Debug)]
struct TracingFileSink {
    writer: NonBlocking,
    guard: WorkerGuard,
    path: PathBuf,
}

/// Metrics naming and bind conventions shared across binaries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
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

/// Supported Prometheus metric kinds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    /// Monotonically increasing counter.
    Counter,
    /// Instantaneous gauge.
    Gauge,
}

/// One rendered metric sample.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricSample {
    /// Fully-qualified metric name.
    pub name: String,
    /// Prometheus metric type.
    pub kind: MetricKind,
    /// Help text for operators.
    pub help: String,
    /// Sorted labels attached to the sample.
    pub labels: BTreeMap<String, String>,
    /// Sample value rendered in exposition output.
    pub value: i64,
}

#[derive(Clone, Debug)]
struct MetricEntry {
    sample: MetricSample,
}

/// Thread-safe registry used by binaries to publish process metrics.
#[derive(Clone, Debug, Default)]
pub struct MetricsRegistry {
    samples: Arc<Mutex<BTreeMap<String, MetricEntry>>>,
}

impl MetricsRegistry {
    /// Creates an empty metrics registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments a counter by the supplied delta.
    pub fn increment_counter(
        &self,
        metrics: &MetricsConfig,
        suffix: &str,
        help: &str,
        labels: &[(&str, &str)],
        delta: u64,
    ) {
        self.update_sample(
            metrics,
            suffix,
            help,
            MetricKind::Counter,
            labels,
            move |value| value.saturating_add(delta as i64),
        );
    }

    /// Sets a gauge to the supplied value.
    pub fn set_gauge(
        &self,
        metrics: &MetricsConfig,
        suffix: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: i64,
    ) {
        self.update_sample(
            metrics,
            suffix,
            help,
            MetricKind::Gauge,
            labels,
            move |_| value,
        );
    }

    /// Returns a stable snapshot of all registered metrics.
    #[must_use]
    pub fn snapshot(&self) -> Vec<MetricSample> {
        let mut samples = self
            .samples
            .lock()
            .expect("metrics registry mutex should not be poisoned")
            .values()
            .map(|entry| entry.sample.clone())
            .collect::<Vec<_>>();
        samples.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.labels.cmp(&right.labels))
        });
        samples
    }

    /// Inserts or replaces one fully-specified metric sample.
    pub fn upsert_sample(&self, sample: MetricSample) {
        let key = sample_key(&sample.name, &sample.labels);
        let mut registry = self
            .samples
            .lock()
            .expect("metrics registry mutex should not be poisoned");
        registry.insert(key, MetricEntry { sample });
    }

    /// Removes one metric sample from the registry when it is present.
    pub fn remove_sample(&self, name: &str, labels: &BTreeMap<String, String>) {
        let key = sample_key(name, labels);
        let mut registry = self
            .samples
            .lock()
            .expect("metrics registry mutex should not be poisoned");
        registry.remove(&key);
    }

    /// Renders the registry in Prometheus exposition format.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();
        for sample in self.snapshot() {
            output.push_str("# HELP ");
            output.push_str(&sample.name);
            output.push(' ');
            output.push_str(&sample.help);
            output.push('\n');

            output.push_str("# TYPE ");
            output.push_str(&sample.name);
            output.push(' ');
            output.push_str(match sample.kind {
                MetricKind::Counter => "counter",
                MetricKind::Gauge => "gauge",
            });
            output.push('\n');

            output.push_str(&sample.name);
            if !sample.labels.is_empty() {
                output.push('{');
                output.push_str(
                    &sample
                        .labels
                        .iter()
                        .map(|(key, value)| format!(r#"{key}="{value}""#))
                        .collect::<Vec<_>>()
                        .join(","),
                );
                output.push('}');
            }
            output.push(' ');
            output.push_str(&sample.value.to_string());
            output.push('\n');
        }
        output
    }

    fn update_sample<F>(
        &self,
        metrics: &MetricsConfig,
        suffix: &str,
        help: &str,
        kind: MetricKind,
        labels: &[(&str, &str)],
        update: F,
    ) where
        F: FnOnce(i64) -> i64,
    {
        let name = metric_name(metrics, suffix);
        let labels = labels
            .iter()
            .map(|(key, value)| (String::from(*key), String::from(*value)))
            .collect::<BTreeMap<_, _>>();
        let key = sample_key(&name, &labels);
        let mut registry = self
            .samples
            .lock()
            .expect("metrics registry mutex should not be poisoned");
        let entry = registry.entry(key).or_insert_with(|| MetricEntry {
            sample: MetricSample {
                name,
                kind,
                help: String::from(help),
                labels,
                value: 0,
            },
        });
        entry.sample.kind = kind;
        entry.sample.help = String::from(help);
        entry.sample.value = update(entry.sample.value);
    }
}

/// Per-process telemetry handle used by binaries for startup metrics and exporters.
#[derive(Clone, Debug)]
pub struct ProcessTelemetry {
    service_name: String,
    metrics: MetricsConfig,
    registry: MetricsRegistry,
}

impl ProcessTelemetry {
    /// Creates a telemetry handle for one process.
    #[must_use]
    pub fn new(service_name: &str, metrics: &MetricsConfig) -> Self {
        Self {
            service_name: String::from(service_name),
            metrics: metrics.clone(),
            registry: MetricsRegistry::new(),
        }
    }

    /// Returns the shared registry.
    #[must_use]
    pub fn registry(&self) -> MetricsRegistry {
        self.registry.clone()
    }

    /// Returns the metrics configuration associated with this process.
    #[must_use]
    pub fn metrics_config(&self) -> MetricsConfig {
        self.metrics.clone()
    }

    /// Emits baseline startup metrics for the process.
    pub fn record_startup(&self) {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs() as i64);
        self.registry.increment_counter(
            &self.metrics,
            "process_start_total",
            "Total process starts observed by this binary.",
            &[("service", self.service_name.as_str())],
            1,
        );
        self.registry.set_gauge(
            &self.metrics,
            "process_start_time_seconds",
            "Unix time when the current process started.",
            &[("service", self.service_name.as_str())],
            start_time,
        );
        self.registry.set_gauge(
            &self.metrics,
            "metrics_exporter_enabled",
            "Whether the process metrics exporter is enabled.",
            &[("service", self.service_name.as_str())],
            i64::from(self.metrics.bind_address.is_some()),
        );
    }

    /// Records one logical lifecycle state value for the process.
    pub fn set_lifecycle_state(&self, state: &str, value: i64) {
        self.registry.set_gauge(
            &self.metrics,
            "lifecycle_state",
            "Current process lifecycle state gauge.",
            &[("service", self.service_name.as_str()), ("state", state)],
            value,
        );
    }

    /// Starts the optional Prometheus endpoint, returning the thread handle when enabled.
    pub fn spawn_exporter(
        &self,
        shutdown: ShutdownToken,
    ) -> Result<Option<JoinHandle<()>>, FoundationError> {
        let Some(bind_address) = &self.metrics.bind_address else {
            return Ok(None);
        };

        let listener = TcpListener::bind(bind_address)?;
        listener.set_nonblocking(true)?;
        let registry = self.registry.clone();
        let bind_address = bind_address.clone();
        let service_name = self.service_name.clone();
        Ok(Some(thread::spawn(move || {
            tracing::info!(
                service = service_name,
                bind_address,
                "metrics exporter listening"
            );
            while !shutdown.is_shutdown_requested() {
                match listener.accept() {
                    Ok((mut stream, _peer)) => {
                        let mut request = [0_u8; 1024];
                        let _ = stream.read(&mut request);
                        let body = registry.render_prometheus();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(error) => {
                        tracing::warn!(
                            service = service_name,
                            error = %error,
                            "metrics exporter accept failed"
                        );
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        })))
    }
}

/// Initializes process-global tracing using shared formatting rules.
pub fn init_tracing(
    service_name: &str,
    tracing_config: &TracingConfig,
) -> Result<TracingGuard, FoundationError> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(tracing_config.level.as_str()))?;
    let file_sink = tracing_file_sink(service_name, tracing_config.log_dir.as_deref())?;
    let file_path = file_sink.as_ref().map(|sink| sink.path.clone());

    let file_guard = if tracing_config.json {
        let formatting = fmt::layer().with_target(true).with_thread_ids(true);
        if let Some(sink) = file_sink {
            let file_layer = fmt::layer()
                .with_writer(sink.writer)
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(true)
                .json()
                .with_current_span(false)
                .flatten_event(true);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    formatting
                        .json()
                        .with_current_span(false)
                        .flatten_event(true),
                )
                .with(file_layer)
                .try_init()?;
            Some(sink.guard)
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    formatting
                        .json()
                        .with_current_span(false)
                        .flatten_event(true),
                )
                .try_init()?;
            None
        }
    } else {
        let formatting = fmt::layer().with_target(true).with_thread_ids(true);
        if let Some(sink) = file_sink {
            let file_layer = fmt::layer()
                .with_writer(sink.writer)
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(true)
                .compact();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(formatting.compact())
                .with(file_layer)
                .try_init()?;
            Some(sink.guard)
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(formatting.compact())
                .try_init()?;
            None
        }
    };

    tracing::info!(
        service = service_name,
        log_path = file_path
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
        "tracing initialized"
    );
    Ok(TracingGuard {
        _file_guard: file_guard,
    })
}

fn tracing_file_sink(
    service_name: &str,
    log_dir: Option<&str>,
) -> Result<Option<TracingFileSink>, FoundationError> {
    let Some(log_dir) = log_dir.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let log_dir = Path::new(log_dir);
    std::fs::create_dir_all(log_dir)?;
    let file_name = format!("{service_name}.log");
    let path = log_dir.join(&file_name);
    let file_appender = tracing_appender::rolling::never(log_dir, file_name);
    let (writer, guard) = tracing_appender::non_blocking(file_appender);
    Ok(Some(TracingFileSink {
        writer,
        guard,
        path,
    }))
}

/// Builds a namespaced metric name from a configured prefix and suffix.
#[must_use]
pub fn metric_name(metrics: &MetricsConfig, suffix: &str) -> String {
    format!("{}_{}", metrics.prefix, suffix)
}

fn sample_key(name: &str, labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return String::from(name);
    }

    format!(
        "{}|{}",
        name,
        labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::{MetricKind, MetricsConfig, MetricsRegistry, ProcessTelemetry, metric_name};
    use crate::ShutdownController;

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

    #[test]
    fn registry_renders_prometheus_with_labels() {
        let metrics = MetricsConfig {
            bind_address: None,
            prefix: String::from("legato"),
        };
        let registry = MetricsRegistry::new();
        registry.increment_counter(
            &metrics,
            "cache_hit_total",
            "Cache hits recorded by the client.",
            &[("service", "legatofs"), ("cache", "extents")],
            2,
        );
        registry.set_gauge(
            &metrics,
            "resident_extents",
            "Resident extent count.",
            &[("service", "legatofs")],
            7,
        );

        let rendered = registry.render_prometheus();

        assert!(rendered.contains("# TYPE legato_cache_hit_total counter"));
        assert!(
            rendered.contains(r#"legato_cache_hit_total{cache="extents",service="legatofs"} 2"#)
        );
        assert!(rendered.contains("# TYPE legato_resident_extents gauge"));
        assert!(rendered.contains(r#"legato_resident_extents{service="legatofs"} 7"#));
    }

    #[test]
    fn process_telemetry_records_startup_metrics_without_exporter() {
        let metrics = MetricsConfig {
            bind_address: None,
            prefix: String::from("legato"),
        };
        let telemetry = ProcessTelemetry::new("legato-server", &metrics);
        let shutdown = ShutdownController::new();

        telemetry.record_startup();
        telemetry.set_lifecycle_state("ready", 1);
        let exporter = telemetry
            .spawn_exporter(shutdown.token())
            .expect("exporter should initialize");

        let samples = telemetry.registry().snapshot();

        assert!(exporter.is_none());
        assert!(samples.iter().any(|sample| {
            sample.kind == MetricKind::Counter && sample.name == "legato_process_start_total"
        }));
        assert!(samples.iter().any(|sample| {
            sample.kind == MetricKind::Gauge
                && sample.name == "legato_lifecycle_state"
                && sample.value == 1
        }));
    }
}

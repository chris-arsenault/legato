//! Server-side runtime metrics for extent fetch and bootstrap activity.

use legato_foundation::ProcessTelemetry;

use crate::{ExtentFetchSource, ReconcileStats};

/// Metrics recorder for server bootstrap and extent-fetch behavior.
#[derive(Clone, Debug)]
pub struct ServerRuntimeMetrics {
    telemetry: ProcessTelemetry,
}

impl ServerRuntimeMetrics {
    /// Creates a recorder backed by the supplied process telemetry.
    #[must_use]
    pub fn new(telemetry: ProcessTelemetry) -> Self {
        Self { telemetry }
    }

    /// Records one library bootstrap reconciliation result.
    pub fn record_bootstrap_reconcile(&self, stats: &ReconcileStats, elapsed_ns: u64) {
        let files_indexed = stats.files_created.saturating_add(stats.files_updated);
        let directories_indexed = stats
            .directories_created
            .saturating_add(stats.directories_updated);
        let records_changed = files_indexed
            .saturating_add(directories_indexed)
            .saturating_add(stats.files_deleted)
            .saturating_add(stats.directories_deleted);
        let registry = self.telemetry.registry();
        let metrics = self.telemetry.metrics_config();
        registry.increment_counter(
            &metrics,
            "server_bootstrap_reconcile_total",
            "Total bootstrap reconciliation passes completed by the server.",
            &[("service", "legato-server")],
            1,
        );
        registry.set_gauge(
            &metrics,
            "server_bootstrap_reconcile_last_duration_ns",
            "Duration of the most recent bootstrap reconciliation pass in nanoseconds.",
            &[("service", "legato-server")],
            elapsed_ns as i64,
        );
        registry.set_gauge(
            &metrics,
            "server_bootstrap_reconcile_last_files_indexed",
            "File count observed in the most recent bootstrap reconciliation pass.",
            &[("service", "legato-server")],
            files_indexed as i64,
        );
        registry.set_gauge(
            &metrics,
            "server_bootstrap_reconcile_last_directories_indexed",
            "Directory count observed in the most recent bootstrap reconciliation pass.",
            &[("service", "legato-server")],
            directories_indexed as i64,
        );
        registry.set_gauge(
            &metrics,
            "server_bootstrap_reconcile_last_records_changed",
            "Catalog records changed during the most recent bootstrap reconciliation pass.",
            &[("service", "legato-server")],
            records_changed as i64,
        );
    }

    /// Records one extent returned by the semantic fetch path.
    pub fn record_extent_fetch(&self, source: ExtentFetchSource, bytes: usize, elapsed_ns: u64) {
        let registry = self.telemetry.registry();
        let metrics = self.telemetry.metrics_config();
        let source_label = match source {
            ExtentFetchSource::CacheHit => "cache_hit",
            ExtentFetchSource::SourceRead => "source_read",
        };

        registry.increment_counter(
            &metrics,
            "server_extent_fetch_total",
            "Total semantic extent fetches completed by the server.",
            &[("service", "legato-server"), ("source", source_label)],
            1,
        );
        registry.increment_counter(
            &metrics,
            "server_extent_fetch_bytes_total",
            "Total extent payload bytes returned by the server.",
            &[("service", "legato-server"), ("source", source_label)],
            bytes as u64,
        );
        registry.set_gauge(
            &metrics,
            "server_extent_fetch_last_duration_ns",
            "Duration of the most recent semantic extent fetch in nanoseconds.",
            &[("service", "legato-server"), ("source", source_label)],
            elapsed_ns as i64,
        );
    }

    /// Exposes the current metric snapshot for tests.
    #[must_use]
    pub fn snapshot(&self) -> Vec<legato_foundation::MetricSample> {
        self.telemetry.registry().snapshot()
    }
}

#[cfg(test)]
mod tests {
    use legato_foundation::{MetricsConfig, ProcessTelemetry};

    use super::ServerRuntimeMetrics;
    use crate::{ExtentFetchSource, ReconcileStats};

    #[test]
    fn extent_fetch_metrics_distinguish_cache_hits_and_source_reads() {
        let telemetry = ProcessTelemetry::new(
            "legato-server",
            &MetricsConfig {
                bind_address: None,
                prefix: String::from("legato"),
            },
        );
        let metrics = ServerRuntimeMetrics::new(telemetry);

        metrics.record_extent_fetch(ExtentFetchSource::SourceRead, 4096, 17);
        metrics.record_extent_fetch(ExtentFetchSource::CacheHit, 4096, 5);

        let snapshot = metrics.snapshot();
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_server_extent_fetch_total"
                && sample
                    .labels
                    .get("source")
                    .is_some_and(|value| value == "source_read")
                && sample.value == 1
        }));
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_server_extent_fetch_total"
                && sample
                    .labels
                    .get("source")
                    .is_some_and(|value| value == "cache_hit")
                && sample.value == 1
        }));
    }

    #[test]
    fn bootstrap_metrics_capture_last_reconcile_shape() {
        let telemetry = ProcessTelemetry::new(
            "legato-server",
            &MetricsConfig {
                bind_address: None,
                prefix: String::from("legato"),
            },
        );
        let metrics = ServerRuntimeMetrics::new(telemetry);
        metrics.record_bootstrap_reconcile(
            &ReconcileStats {
                directories_created: 5,
                files_created: 30,
                directories_updated: 7,
                files_updated: 12,
                directories_deleted: 1,
                files_deleted: 2,
            },
            1_000,
        );

        let snapshot = metrics.snapshot();
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_server_bootstrap_reconcile_last_files_indexed"
                && sample.value == 42
        }));
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_server_bootstrap_reconcile_last_duration_ns"
                && sample.value == 1_000
        }));
    }
}

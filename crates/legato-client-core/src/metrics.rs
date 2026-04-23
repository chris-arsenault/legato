//! Client-side runtime metrics for cache, reconnect, invalidation, and prefetch behavior.

use legato_client_cache::client_store::ClientStoreMaintenanceReport;
use legato_foundation::{MetricSample, MetricsConfig, MetricsRegistry, ProcessTelemetry};
use legato_proto::{InvalidationEvent, InvalidationKind};

/// Aggregated summary for one prefetch execution pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrefetchMetricsReport {
    /// Hints accepted for processing.
    pub accepted: u64,
    /// Hints skipped because their data was already resident.
    pub skipped: u64,
    /// Hints completed successfully.
    pub completed: u64,
    /// Hints that failed during execution.
    pub failed: u64,
    /// Total bytes read during the pass.
    pub bytes_read: u64,
    /// Net new bytes warmed into the local extent store.
    pub bytes_fetched: u64,
    /// End-to-end pass duration in nanoseconds.
    pub elapsed_ns: u64,
}

/// Shared metrics recorder used by mounted clients and prefetch runtimes.
#[derive(Clone, Debug)]
pub struct ClientRuntimeMetrics {
    service_name: String,
    metrics: MetricsConfig,
    registry: MetricsRegistry,
}

impl ClientRuntimeMetrics {
    /// Creates a metrics recorder that publishes into the supplied process telemetry.
    #[must_use]
    pub fn new(service_name: impl Into<String>, telemetry: &ProcessTelemetry) -> Self {
        Self {
            service_name: service_name.into(),
            metrics: telemetry.metrics_config(),
            registry: telemetry.registry(),
        }
    }

    /// Records one logical client read with cache hit/miss shape and latency.
    pub fn record_read(
        &self,
        cache_hits: u64,
        cache_misses: u64,
        local_bytes: u64,
        remote_bytes: u64,
        elapsed_ns: u64,
    ) {
        let service = self.service_name.as_str();
        self.registry.increment_counter(
            &self.metrics,
            "client_read_total",
            "Total logical client reads completed by the runtime.",
            &[("service", service)],
            1,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_cache_extent_total",
            "Total extent cache hit decisions made by the client runtime.",
            &[("service", service), ("result", "hit")],
            cache_hits,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_cache_extent_total",
            "Total extent cache miss decisions made by the client runtime.",
            &[("service", service), ("result", "miss")],
            cache_misses,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_read_bytes_total",
            "Total logical read bytes served from resident local extents.",
            &[("service", service), ("source", "local")],
            local_bytes,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_read_bytes_total",
            "Total logical read bytes that required remote fetch before completion.",
            &[("service", service), ("source", "remote")],
            remote_bytes,
        );
        self.registry.set_gauge(
            &self.metrics,
            "client_read_last_duration_ns",
            "Duration of the most recent logical client read in nanoseconds.",
            &[("service", service)],
            elapsed_ns as i64,
        );
    }

    /// Records one reconnect event and its completion latency.
    pub fn record_reconnect(&self, elapsed_ns: u64) {
        let service = self.service_name.as_str();
        self.registry.increment_counter(
            &self.metrics,
            "client_reconnect_total",
            "Total client reconnect attempts completed successfully.",
            &[("service", service)],
            1,
        );
        self.registry.set_gauge(
            &self.metrics,
            "client_reconnect_last_duration_ns",
            "Duration of the most recent successful client reconnect in nanoseconds.",
            &[("service", service)],
            elapsed_ns as i64,
        );
    }

    /// Records one invalidation and the lag between server emission and local handling.
    pub fn record_invalidation(&self, event: &InvalidationEvent, handled_at_ns: u64) {
        let service = self.service_name.as_str();
        let lag_ns = handled_at_ns.saturating_sub(event.issued_at_ns);
        self.registry.increment_counter(
            &self.metrics,
            "client_invalidation_total",
            "Total invalidation events applied by the client runtime.",
            &[
                ("service", service),
                (
                    "kind",
                    match InvalidationKind::try_from(event.kind)
                        .unwrap_or(InvalidationKind::Unspecified)
                    {
                        InvalidationKind::File => "file",
                        InvalidationKind::Directory => "directory",
                        InvalidationKind::Subtree => "subtree",
                        InvalidationKind::Unspecified => "unspecified",
                    },
                ),
            ],
            1,
        );
        self.registry.set_gauge(
            &self.metrics,
            "client_invalidation_last_lag_ns",
            "Lag between invalidation emission and local client handling in nanoseconds.",
            &[("service", service)],
            lag_ns as i64,
        );
    }

    /// Records one prefetch execution summary.
    pub fn record_prefetch(&self, report: PrefetchMetricsReport) {
        let service = self.service_name.as_str();
        for (status, value, help) in [
            (
                "accepted",
                report.accepted,
                "Total prefetch hints accepted for processing.",
            ),
            (
                "skipped",
                report.skipped,
                "Total prefetch hints skipped because data was already resident.",
            ),
            (
                "completed",
                report.completed,
                "Total prefetch hints completed successfully.",
            ),
            (
                "failed",
                report.failed,
                "Total prefetch hints that failed during execution.",
            ),
        ] {
            self.registry.increment_counter(
                &self.metrics,
                "client_prefetch_hints_total",
                help,
                &[("service", service), ("status", status)],
                value,
            );
        }
        self.registry.increment_counter(
            &self.metrics,
            "client_prefetch_bytes_total",
            "Total bytes read while executing client-side prefetch work.",
            &[("service", service), ("kind", "read")],
            report.bytes_read,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_prefetch_bytes_total",
            "Total bytes newly warmed into the local extent store during prefetch work.",
            &[("service", service), ("kind", "fetched")],
            report.bytes_fetched,
        );
        self.registry.set_gauge(
            &self.metrics,
            "client_prefetch_last_duration_ns",
            "Duration of the most recent prefetch execution in nanoseconds.",
            &[("service", service)],
            report.elapsed_ns as i64,
        );
    }

    /// Records automatic eviction activity and updates current residency gauges.
    pub fn record_eviction(&self, report: &ClientStoreMaintenanceReport) {
        let service = self.service_name.as_str();
        self.registry.increment_counter(
            &self.metrics,
            "client_eviction_total",
            "Total automatic cache-eviction passes completed by the client runtime.",
            &[("service", service)],
            1,
        );
        self.registry.increment_counter(
            &self.metrics,
            "client_eviction_bytes_total",
            "Total resident bytes removed by automatic client eviction.",
            &[("service", service)],
            report.resident_bytes_removed,
        );
        self.record_residency(
            report.resident_bytes_after,
            report.resident_extents_after as u64,
        );
    }

    /// Records one compaction or checkpoint-style maintenance pass.
    pub fn record_compaction(&self, report: &ClientStoreMaintenanceReport) {
        let service = self.service_name.as_str();
        self.registry.increment_counter(
            &self.metrics,
            "client_compaction_total",
            "Total client cache compaction or checkpoint maintenance passes completed.",
            &[("service", service)],
            1,
        );
        self.record_residency(
            report.resident_bytes_after,
            report.resident_extents_after as u64,
        );
    }

    /// Updates current local residency gauges.
    pub fn record_residency(&self, resident_bytes: u64, resident_extents: u64) {
        let service = self.service_name.as_str();
        self.registry.set_gauge(
            &self.metrics,
            "client_resident_bytes",
            "Current logical resident payload bytes in the local extent store.",
            &[("service", service)],
            resident_bytes as i64,
        );
        self.registry.set_gauge(
            &self.metrics,
            "client_resident_extents",
            "Current resident extent references in the local extent store.",
            &[("service", service)],
            resident_extents as i64,
        );
    }

    /// Exposes the current metric snapshot for tests.
    #[must_use]
    pub fn snapshot(&self) -> Vec<MetricSample> {
        self.registry.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use legato_foundation::{MetricsConfig, ProcessTelemetry};
    use legato_proto::{InvalidationEvent, InvalidationKind};

    use super::{ClientRuntimeMetrics, PrefetchMetricsReport};

    #[test]
    fn runtime_metrics_record_cache_prefetch_and_invalidation_activity() {
        let telemetry = ProcessTelemetry::new(
            "legatofs",
            &MetricsConfig {
                bind_address: None,
                prefix: String::from("legato"),
            },
        );
        let metrics = ClientRuntimeMetrics::new("legatofs", &telemetry);
        metrics.record_read(2, 1, 4096, 1024, 17);
        metrics.record_reconnect(23);
        metrics.record_invalidation(
            &InvalidationEvent {
                kind: InvalidationKind::Subtree as i32,
                path: String::from("/Kontakt"),
                file_id: 7,
                issued_at_ns: 100,
            },
            160,
        );
        metrics.record_prefetch(PrefetchMetricsReport {
            accepted: 3,
            skipped: 1,
            completed: 2,
            failed: 1,
            bytes_read: 8192,
            bytes_fetched: 4096,
            elapsed_ns: 31,
        });
        metrics.record_residency(4096, 2);

        let snapshot = metrics.snapshot();
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_client_cache_extent_total"
                && sample
                    .labels
                    .get("result")
                    .is_some_and(|value| value == "hit")
                && sample.value == 2
        }));
        assert!(
            snapshot.iter().any(|sample| {
                sample.name == "legato_client_reconnect_total" && sample.value == 1
            })
        );
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_client_invalidation_last_lag_ns" && sample.value == 60
        }));
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_client_prefetch_bytes_total"
                && sample
                    .labels
                    .get("kind")
                    .is_some_and(|value| value == "fetched")
                && sample.value == 4096
        }));
        assert!(snapshot.iter().any(|sample| {
            sample.name == "legato_client_resident_bytes" && sample.value == 4096
        }));
    }
}

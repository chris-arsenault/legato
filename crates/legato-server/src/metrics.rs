//! Server-side runtime metrics for extent fetch, bootstrap activity, and unified client metrics.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use legato_foundation::{MetricSample, ProcessTelemetry, ShutdownToken};

use crate::{ExtentFetchSource, ReconcileStats};

const CLIENT_METRICS_STALE_AFTER_SECS: u64 = 300;
const CLIENT_METRICS_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct TrackedClientMetric {
    client_name: String,
    service_name: String,
    sample: MetricSample,
    last_seen_unix_seconds: u64,
}

/// Metrics recorder for server bootstrap, extent-fetch behavior, and aggregated client metrics.
#[derive(Clone, Debug)]
pub struct ServerRuntimeMetrics {
    telemetry: ProcessTelemetry,
    client_metrics: Arc<Mutex<BTreeMap<String, TrackedClientMetric>>>,
}

impl ServerRuntimeMetrics {
    /// Creates a recorder backed by the supplied process telemetry.
    #[must_use]
    pub fn new(telemetry: ProcessTelemetry) -> Self {
        Self {
            telemetry,
            client_metrics: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Spawns background cleanup for stale client metric series.
    pub fn spawn_client_metrics_cleanup(&self, shutdown: ShutdownToken) -> JoinHandle<()> {
        let metrics = self.clone();
        thread::spawn(move || {
            while !shutdown.is_shutdown_requested() {
                metrics.prune_stale_client_metrics();
                thread::sleep(CLIENT_METRICS_CLEANUP_INTERVAL);
            }
        })
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

    /// Records one authoritative full snapshot reported by a client runtime.
    pub fn record_client_snapshot(&self, client_name: &str, samples: &[MetricSample]) -> usize {
        self.record_client_snapshot_at(client_name, samples, now_unix_seconds())
    }

    /// Removes stale client metric series from the exported registry.
    pub fn prune_stale_client_metrics(&self) {
        self.prune_stale_client_metrics_at(now_unix_seconds());
    }

    /// Exposes the current metric snapshot for tests.
    #[must_use]
    pub fn snapshot(&self) -> Vec<MetricSample> {
        self.telemetry.registry().snapshot()
    }

    fn record_client_snapshot_at(
        &self,
        client_name: &str,
        samples: &[MetricSample],
        now_unix_seconds: u64,
    ) -> usize {
        self.prune_stale_client_metrics_at(now_unix_seconds);
        let registry = self.telemetry.registry();
        let mut tracked = self
            .client_metrics
            .lock()
            .expect("client metrics mutex should not be poisoned");
        let mut seen_keys = BTreeSet::new();
        let mut reported_services = BTreeSet::new();

        for sample in samples {
            let enriched = enrich_client_sample(client_name, sample);
            let key = tracked_sample_key(&enriched.name, &enriched.labels);
            let service_name = enriched.labels.get("service").cloned().unwrap_or_default();
            registry.upsert_sample(enriched.clone());
            tracked.insert(
                key.clone(),
                TrackedClientMetric {
                    client_name: String::from(client_name),
                    service_name: service_name.clone(),
                    sample: enriched,
                    last_seen_unix_seconds: now_unix_seconds,
                },
            );
            seen_keys.insert(key);
            reported_services.insert(service_name);
        }

        let obsolete = tracked
            .iter()
            .filter(|(key, entry)| {
                entry.client_name == client_name
                    && reported_services.contains(&entry.service_name)
                    && !seen_keys.contains(*key)
            })
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.sample.name.clone(),
                    entry.sample.labels.clone(),
                )
            })
            .collect::<Vec<_>>();
        for (key, name, labels) in obsolete {
            tracked.remove(&key);
            registry.remove_sample(&name, &labels);
        }

        samples.len()
    }

    fn prune_stale_client_metrics_at(&self, now_unix_seconds: u64) {
        let registry = self.telemetry.registry();
        let mut tracked = self
            .client_metrics
            .lock()
            .expect("client metrics mutex should not be poisoned");
        let expired = tracked
            .iter()
            .filter(|(_key, entry)| {
                now_unix_seconds.saturating_sub(entry.last_seen_unix_seconds)
                    > CLIENT_METRICS_STALE_AFTER_SECS
            })
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.sample.name.clone(),
                    entry.sample.labels.clone(),
                )
            })
            .collect::<Vec<_>>();
        for (key, name, labels) in expired {
            tracked.remove(&key);
            registry.remove_sample(&name, &labels);
        }
    }
}

fn enrich_client_sample(client_name: &str, sample: &MetricSample) -> MetricSample {
    let mut labels = sample.labels.clone();
    labels.insert(String::from("client_name"), String::from(client_name));
    MetricSample {
        name: sample.name.clone(),
        kind: sample.kind,
        help: sample.help.clone(),
        labels,
        value: sample.value,
    }
}

fn tracked_sample_key(name: &str, labels: &BTreeMap<String, String>) -> String {
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

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use legato_foundation::{MetricKind, MetricsConfig, ProcessTelemetry};

    use super::ServerRuntimeMetrics;
    use crate::{ExtentFetchSource, ReconcileStats};

    fn sample(name: &str, value: i64) -> legato_foundation::MetricSample {
        let mut labels = BTreeMap::new();
        labels.insert(String::from("service"), String::from("legatofs"));
        legato_foundation::MetricSample {
            name: String::from(name),
            kind: MetricKind::Counter,
            help: String::from("test sample"),
            labels,
            value,
        }
    }

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

    #[test]
    fn client_metric_snapshots_replace_existing_counter_values() {
        let telemetry = ProcessTelemetry::new(
            "legato-server",
            &MetricsConfig {
                bind_address: None,
                prefix: String::from("legato"),
            },
        );
        let metrics = ServerRuntimeMetrics::new(telemetry);

        assert_eq!(
            metrics.record_client_snapshot_at(
                "studio-mac",
                &[sample("legatofs_client_read_total", 3)],
                10
            ),
            1
        );
        assert_eq!(
            metrics.record_client_snapshot_at(
                "studio-mac",
                &[sample("legatofs_client_read_total", 5)],
                20
            ),
            1
        );

        let snapshot = metrics.snapshot();
        let matches = snapshot
            .iter()
            .filter(|sample| {
                sample.name == "legatofs_client_read_total"
                    && sample
                        .labels
                        .get("client_name")
                        .is_some_and(|value| value == "studio-mac")
            })
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].value, 5);
    }

    #[test]
    fn stale_client_metric_series_are_pruned_from_snapshot() {
        let telemetry = ProcessTelemetry::new(
            "legato-server",
            &MetricsConfig {
                bind_address: None,
                prefix: String::from("legato"),
            },
        );
        let metrics = ServerRuntimeMetrics::new(telemetry);
        metrics.record_client_snapshot_at(
            "studio-win",
            &[sample("legatofs_client_read_total", 7)],
            10,
        );

        metrics.prune_stale_client_metrics_at(10 + super::CLIENT_METRICS_STALE_AFTER_SECS + 1);

        assert!(!metrics.snapshot().iter().any(|sample| {
            sample.name == "legatofs_client_read_total"
                && sample
                    .labels
                    .get("client_name")
                    .is_some_and(|value| value == "studio-win")
        }));
    }
}

# Monitoring

This document captures the current monitoring and observability shape of Legato as implemented today.

## Summary

Legato currently exposes:

- structured logs through `tracing`
- Prometheus-style metrics through a small in-process HTTP exporter

Legato does not currently expose:

- OTLP
- StatsD
- Influx line protocol
- direct Elasticsearch or Kibana metric sinks
- direct InfluxDB metric sinks

## Log Pipeline

All binaries initialize tracing through `legato-foundation` and can emit either compact text logs or structured JSON logs.

Relevant implementation:

- `crates/legato-foundation/src/telemetry.rs`
  `init_tracing`

Operationally:

- `common.tracing.json = true` is the right mode for shipping logs into an Elastic stack
- JSON logs can be collected by normal log shippers such as Filebeat, Elastic Agent, or Fluent Bit
- this makes Kibana a good fit for log search and incident investigation

The server example already defaults to JSON tracing:

- `deploy/server/server.toml.example`

## Metrics Pipeline

Metrics are implemented as an in-process registry plus a simple HTTP listener that renders Prometheus exposition text.

Relevant implementation:

- `crates/legato-foundation/src/telemetry.rs`
  `MetricsRegistry`
- `crates/legato-foundation/src/telemetry.rs`
  `render_prometheus`
- `crates/legato-foundation/src/telemetry.rs`
  `ProcessTelemetry::spawn_exporter`

Metrics are enabled by setting:

- `common.metrics.bind_address`

Example:

- `deploy/server/server.toml.example`
  `common.metrics.bind_address = "0.0.0.0:9464"`

The current exporter:

- binds a plain TCP listener
- serves Prometheus-style text over HTTP
- has no auth
- has no TLS
- should only be exposed on trusted networks

## What Exists Today

### Common Process Metrics

Every binary currently emits baseline process metrics:

- `*_process_start_total`
- `*_process_start_time_seconds`
- `*_metrics_exporter_enabled`
- `*_lifecycle_state{state="bootstrap|ready"}`

These come from:

- `ProcessTelemetry::record_startup`
- `ProcessTelemetry::set_lifecycle_state`

### Server Metrics

The server is the only binary with meaningful workload metrics today.

Relevant implementation:

- `crates/legato-server/src/metrics.rs`

Current server metrics include:

- bootstrap reconcile count
- last bootstrap reconcile duration
- last files indexed
- last directories indexed
- last records changed
- extent fetch count
- extent fetch bytes
- last extent fetch duration
- extent fetch source labels: `cache_hit` and `source_read`

This is the useful performance surface today.

### Client And Prefetch Metrics

`legatofs` and `legato-prefetch` now emit runtime metrics in addition to startup and lifecycle metrics.

Current client-side runtime metrics include:

- logical client read count
- extent cache hit and miss counts
- logical read bytes served locally vs bytes that required remote fetch
- last client read duration
- reconnect count and last reconnect duration
- invalidation count by kind and last invalidation handling lag
- resident extent and resident byte gauges
- automatic eviction count and bytes removed
- prefetch hints accepted, skipped, completed, and failed
- prefetch bytes read and bytes newly warmed
- last prefetch duration

Current limitations remain:

- no per-file or top-N hot/cold file metrics
- no direct cache inventory endpoint
- no unified server aggregation endpoint yet
- no first-class compaction metrics emitted from explicit maintenance commands yet

## Proposed Unified Metrics Endpoint

The preferred future shape is a single scrape surface on the server.

That model would work like this:

- clients and prefetch workers emit their local runtime metrics as they do work
- those metrics are forwarded to the server rather than scraped from each client directly
- the server maintains an aggregated registry that includes both server-local and client-reported metrics
- Influx, Prometheus, or any other downstream collector reads only from the server metrics endpoint

Operationally, that gives Legato one canonical metrics endpoint:

- `legato-server:/metrics`

Instead of:

- one metrics endpoint per client machine
- separate scraping and service discovery for laptops and workstations
- partial visibility when a client exporter is disabled or unreachable

This is the right shape for this project because the server is already the stable always-on component, while clients are comparatively ephemeral.

### Proposed Behavior

Under this model, clients would report metrics such as:

- cache hit and miss counts
- bytes read locally vs fetched remotely
- prefetch requests accepted, completed, and failed
- prefetch duration and bytes warmed
- reconnect count and reconnect duration
- invalidation count and invalidation handling lag
- cache eviction and compaction activity

The server would then expose:

- its own local metrics
- per-client metrics labeled by client identity
- fleet-wide rolled-up counters and gauges where aggregation is meaningful

Useful labels would include:

- `client_name`
- `host`
- `platform`
- `mount_id`
- `library`

### Proposal Constraints

To keep the unified endpoint correct, the aggregation model should follow a few rules:

- monotonic counters should be reported as deltas or otherwise deduplicated so reconnecting clients do not double-count
- gauges should carry freshness semantics so stale disconnected clients do not look healthy forever
- per-client series should age out after a timeout if the server stops hearing from that client
- the server should preserve enough label information to debug one machine without losing the ability to graph totals

### Sink Topology

With this shape:

- clients write metrics to the server
- the server exposes one unified metrics endpoint
- Influx scrapes the server endpoint, either directly through Prometheus scrape support or through Telegraf
- Kibana still remains primarily a log destination unless metrics are separately bridged into Elasticsearch

This would materially simplify operations because metric ingestion would no longer depend on discovering and scraping every active client machine.

## InfluxDB

Legato does not write directly to InfluxDB.

Legato can still feed Influx if you use a bridge that scrapes Prometheus-style metrics and forwards them to Influx. Typical approaches are:

- Telegraf scraping `/metrics` and writing to InfluxDB
- InfluxDB Prometheus scrape support, if enabled in your deployment

So the answer for Influx is:

- yes through Prometheus scraping and translation
- no as a direct native sink

## Kibana

Kibana is a good fit for logs today.

If `common.tracing.json = true`, Legato emits structured JSON logs that can be shipped into Elasticsearch and viewed in Kibana.

For metrics, Kibana only works indirectly. You would need external tooling that scrapes the Prometheus endpoint and forwards those metrics into Elasticsearch.

So the answer for Kibana is:

- yes for logs
- only indirectly for metrics

## Configuration Notes

Current examples:

- server example enables JSON logs and a metrics bind address:
  `deploy/server/server.toml.example`
- client example sets a metrics prefix but does not enable a metrics bind address by default:
  `deploy/client/config/legatofs.toml.example`

This means:

- server-side metrics are intended to be scraped today
- client-side metrics are possible in code but not enabled by default in the shipped example config

## Naming Notes

Metric names are built as:

- `prefix + "_" + suffix`

Because the server example uses `prefix = "legato_server"` and server-specific metric suffixes already begin with `server_`, current names can be redundant, for example:

- `legato_server_server_extent_fetch_total`

This is not functionally wrong, but it is operationally a bit awkward.

## Operational Verdict

Current monitoring support is real but limited.

What is good enough today:

- structured server logs into an Elastic/Kibana stack
- Prometheus-style scraping of server metrics
- bridging those metrics into Influx if desired

What is not yet mature:

- client-side performance monitoring
- prefetch effectiveness monitoring
- richer alerting-oriented metrics around reconnects, cache behavior, invalidation handling, and background maintenance

## Bottom Line

Today:

- Kibana is a strong fit for logs
- Influx is workable for metrics through a Prometheus bridge
- Legato does not natively integrate with either sink directly

The larger current limitation is not export compatibility, it is metric depth. The server exposes some useful workload metrics, but the client and prefetch paths still have sparse operational visibility.

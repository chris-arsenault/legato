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

## Unified Metrics Endpoint

Legato now supports a single canonical scrape surface on the server.

Current behavior:

- clients and prefetch workers still emit metrics into their local in-process registry
- the shared client runtime periodically reports full metric snapshots upstream to the server
- the server stores fresh client samples in its own registry with `client_name` labels attached
- the server metrics exporter exposes both server-local and client-reported metrics on one endpoint

Operationally, that gives Legato one canonical metrics endpoint:

- `legato-server:/metrics`

instead of requiring one scrape target per workstation.

### Current Behavior

Client-reported metrics forwarded to the server include:

- cache hit and miss counts
- bytes read locally vs fetched remotely
- prefetch requests accepted, completed, and failed
- prefetch duration and bytes warmed
- reconnect count and reconnect duration
- invalidation count and invalidation handling lag
- automatic cache eviction activity

The server now exposes:

- its own local metrics
- per-client metrics labeled by client identity

Each client-reported sample is exported with:

- the original metric name
- the original metric labels
- an added `client_name` label

Useful labels would include:

- `client_name`
- `service`

### Proposal Constraints

The aggregation model currently follows these rules:

- clients report full snapshots, not deltas, so reconnects replace the current value instead of incrementing the server registry twice
- stale client series are pruned after a timeout if the server stops hearing from that client
- per-client label fidelity is preserved so one host can still be isolated in graphs or queries

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

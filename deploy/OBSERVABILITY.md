# Legato Observability

This guide explains how to connect Legato logs and metrics to Elastic/Kibana.

Legato does not talk to Kibana directly. It emits process logs and Prometheus-format metrics; an Elastic Agent, Filebeat, Metricbeat, or similar collector ships those signals into Elasticsearch, and Kibana reads them from there.

## Signal Sources

Legato emits:

- structured logs through `tracing`
- Prometheus-format metrics from an optional HTTP listener
- client runtime metrics that can be reported back to the server and exported with server metrics

Useful metrics include:

- `legato_server_process_start_total`
- `legato_server_lifecycle_state`
- `legato_server_server_extent_fetch_total`
- `legato_server_server_extent_fetch_bytes_total`
- `legato_server_server_bootstrap_reconcile_total`
- `legatofs_client_read_total`
- `legatofs_client_cache_extent_total`
- `legatofs_client_reconnect_total`
- `legatofs_client_prefetch_hints_total`
- `legatofs_client_resident_bytes`

Metric names include the configured prefix. The server example uses `legato_server`; the client example uses `legatofs`.

## Server Configuration

The server example config is [server.toml.example](server/server.toml.example).

Enable JSON logs and metrics:

```toml
[common.tracing]
json = true
level = "info"

[common.metrics]
bind_address = "0.0.0.0:9464"
prefix = "legato_server"
```

The same settings can be supplied as environment variables:

```bash
LEGATO_SERVER__COMMON__TRACING__JSON=true
LEGATO_SERVER__COMMON__TRACING__LEVEL=info
LEGATO_SERVER__COMMON__METRICS__BIND_ADDRESS=0.0.0.0:9464
LEGATO_SERVER__COMMON__METRICS__PREFIX=legato_server
```

Keep the metrics listener on a trusted network. In a TrueNAS/Komodo deployment, expose or route `9464` only to the collector that scrapes it.

## Client Configuration

Client config is generated at install time. To emit JSON service logs, edit `legatofs.toml` after registration:

```toml
[common.tracing]
json = true
level = "info"
```

To expose a client-local metrics endpoint, add a bind address:

```toml
[common.metrics]
bind_address = "127.0.0.1:9465"
prefix = "legatofs"
```

Client metrics are also reported to the server during normal runtime and exported by the server metrics endpoint with a `client_name` label when server metrics are enabled.

## Elastic/Kibana Integration

Use Elastic Agent or Beats in two lanes:

- Logs: ship container stdout/stderr for `legato-server`, plus client service log files.
- Metrics: scrape the Prometheus endpoint exposed by `common.metrics.bind_address`.

Recommended Kibana data views:

- `logs-*` for server and client logs
- `metrics-*` for Prometheus metrics

Recommended dimensions:

- `service`
- `client_name`
- `source`
- `result`
- `status`
- `kind`

## Server Logs

For the server container, configure the collector to ingest Docker or container logs from the TrueNAS host. With `common.tracing.json=true`, each Legato log event is already structured JSON.

Filter in Kibana by:

```text
service: "legato-server"
```

Useful searches:

```text
message: "metrics exporter listening"
```

```text
level: "WARN" or level: "ERROR"
```

## Client Logs

macOS service logs:

```text
~/Library/Logs/Legato/legatofs.out.log
~/Library/Logs/Legato/legatofs.err.log
```

Windows service logs:

```text
C:\ProgramData\Legato\logs\legatofs.out.log
C:\ProgramData\Legato\logs\legatofs.err.log
```

Configure Elastic Agent or Filebeat to collect those paths from each client host. If `json = true`, parse them as newline-delimited JSON; otherwise ingest them as plain text.

## Prometheus Metrics

Scrape the server:

```text
http://legato.lan:9464/metrics
```

If client-local metrics are enabled, scrape each client locally or through a trusted management network:

```text
http://127.0.0.1:9465/metrics
```

In Elastic Agent, use the Prometheus integration with the metrics path set to:

```text
/metrics
```

Use a short interval, such as `15s` or `30s`, for interactive dashboards. Use a longer interval if the deployment is small and alert latency is not important.

## Starter Dashboards

Create panels for:

- Server process up: latest `legato_server_lifecycle_state{state="ready"}`
- Server extent fetch count by `source`
- Server extent fetch bytes by `source`
- Client read count by `client_name`
- Client cache hit and miss count by `result`
- Client reconnect count by `client_name`
- Client resident bytes by `client_name`
- Prefetch hints by `status`

Starter alerts:

- no server metrics received for 5 minutes
- `legatofs_client_reconnect_total` increases repeatedly for a client
- cache miss count stays high during playback
- `legatofs_client_prefetch_hints_total{status="failed"}` increases
- resident bytes approaches the configured client cache limit

## Troubleshooting

Check that the server is exposing metrics:

```bash
curl http://legato.lan:9464/metrics
```

Check whether the server emitted the metrics listener log:

```text
metrics exporter listening
```

If Kibana shows logs but no metrics, inspect the collector first. Legato metrics are served as plain Prometheus text; there is no Elasticsearch-specific exporter in the binary.

If metrics exist but client labels are missing, confirm clients are connected long enough to report their metric snapshots to the server.

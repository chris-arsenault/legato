# Legato Observability

The default deployment exposes Prometheus metrics from `legato-server` on port `9464`.

For Kibana/Elastic, the normal setup is one action: register a Prometheus scraper for the server endpoint.

```text
http://legato.lan:9464/metrics
```

If your server is not named `legato.lan`, use the server host or IP instead.

## What Gets Scraped

The server endpoint includes server metrics and client metrics reported by connected clients.

Useful series include:

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

Client metrics include a `client_name` label when they are exported through the server.

## Kibana Setup

Use Elastic Agent's Prometheus integration.

Configure:

- Host: `http://legato.lan:9464`
- Metrics path: `/metrics`
- Scrape interval: `15s` or `30s`

Useful dashboard dimensions:

- `service`
- `client_name`
- `source`
- `result`
- `status`
- `kind`

Suggested panels:

- server ready state from `legato_server_lifecycle_state`
- server extent fetches by `source`
- client reads by `client_name`
- client cache hits and misses by `result`
- reconnects by `client_name`
- resident bytes by `client_name`
- prefetch hints by `status`

Suggested alerts:

- no server scrape for 5 minutes
- reconnect count keeps increasing for a client
- cache misses stay high during playback
- prefetch failures increase
- resident bytes approaches the intended client cache limit

## Logs

The default compose stack emits structured JSON logs from the server container. If you already collect container logs into Elastic, filter for:

```text
service: "legato-server"
```

Client logs are local host diagnostics:

- macOS: `~/Library/Logs/Legato/`
- Windows: `C:\ProgramData\Legato\logs\`

You do not need client log shipping for the normal Kibana dashboard because client runtime metrics flow back through the server metrics endpoint.

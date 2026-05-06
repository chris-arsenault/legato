FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --home /nonexistent --shell /usr/sbin/nologin --uid 10001 legato

WORKDIR /app
COPY dist/legato-server /usr/local/bin/legato-server

ENV LEGATO_SERVER__COMMON__TRACING__JSON=true
ENV LEGATO_SERVER__COMMON__TRACING__LEVEL=info
ENV LEGATO_SERVER__COMMON__TRACING__LOG_DIR=/var/lib/legato/logs
ENV LEGATO_SERVER__COMMON__METRICS__BIND_ADDRESS=0.0.0.0:9464
ENV LEGATO_SERVER__COMMON__METRICS__PREFIX=legato_server

VOLUME ["/srv/libraries", "/var/lib/legato", "/etc/legato"]

USER legato
EXPOSE 7823 7824 9464
EXPOSE 7825/udp

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/legato-server"]

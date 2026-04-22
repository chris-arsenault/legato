FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --home /nonexistent --shell /usr/sbin/nologin --uid 10001 legato

WORKDIR /app
COPY dist/legato-server /usr/local/bin/legato-server

ENV LEGATO_SERVER__COMMON__TRACING__JSON=true
ENV LEGATO_SERVER__COMMON__TRACING__LEVEL=info

VOLUME ["/srv/libraries", "/var/lib/legato", "/etc/legato"]

USER legato
EXPOSE 7823

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/legato-server"]

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d)"
PORT="${LEGATO_SMOKE_PORT:-17823}"
SERVER_LOG="${WORK_DIR}/server.log"
LIBRARY_ROOT="${WORK_DIR}/library"
SERVER_STATE="${WORK_DIR}/server-state"
TLS_DIR="${WORK_DIR}/tls"
CLIENT_BUNDLE="${WORK_DIR}/bundle"
CLIENT_STATE="${WORK_DIR}/client-state"
SAMPLE_PATH="${LIBRARY_ROOT}/Kontakt/piano.nki"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${LIBRARY_ROOT}/Kontakt" "${SERVER_STATE}" "${TLS_DIR}"
printf 'hello legato smoke\n' > "${SAMPLE_PATH}"

server_env() {
  env \
    LEGATO_SERVER__SERVER__BIND_ADDRESS="127.0.0.1:${PORT}" \
    LEGATO_SERVER__SERVER__LIBRARY_ROOT="${LIBRARY_ROOT}" \
    LEGATO_SERVER__SERVER__STATE_DIR="${SERVER_STATE}" \
    LEGATO_SERVER__SERVER__TLS_DIR="${TLS_DIR}" \
    LEGATO_SERVER__SERVER__TLS__CERT_PATH="${TLS_DIR}/server.pem" \
    LEGATO_SERVER__SERVER__TLS__KEY_PATH="${TLS_DIR}/server-key.pem" \
    LEGATO_SERVER__SERVER__TLS__CLIENT_CA_PATH="${TLS_DIR}/client-ca.pem" \
    "$@"
}

start_server() {
  server_env "${ROOT_DIR}/target/release/legato-server" >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 40); do
    if grep -q "legato-server bootstrap ready" "${SERVER_LOG}" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done

  echo "server failed to start" >&2
  cat "${SERVER_LOG}" >&2 || true
  return 1
}

start_server

server_env "${ROOT_DIR}/target/release/legato-server" issue-client \
  --name "release-smoke" \
  --output-dir "${CLIENT_BUNDLE}" \
  --endpoint "localhost:${PORT}" \
  --server-name "localhost"

"${ROOT_DIR}/target/release/legatofs" install \
  --bundle-dir "${CLIENT_BUNDLE}" \
  --mount-point "/tmp/legato-smoke" \
  --state-dir "${CLIENT_STATE}" \
  --library-root "${LIBRARY_ROOT}"

"${ROOT_DIR}/target/release/legatofs" smoke \
  --config "${CLIENT_STATE}/legatofs.toml" \
  --path "${SAMPLE_PATH}" \
  --size 8

kill "${SERVER_PID}"
wait "${SERVER_PID}" || true
unset SERVER_PID

PORT="$((PORT + 1))"
SERVER_STATE="${WORK_DIR}/server-state-restart"
mkdir -p "${SERVER_STATE}"
start_server

"${ROOT_DIR}/target/release/legatofs" install \
  --bundle-dir "${CLIENT_BUNDLE}" \
  --endpoint "localhost:${PORT}" \
  --server-name "localhost" \
  --mount-point "/tmp/legato-smoke" \
  --state-dir "${CLIENT_STATE}" \
  --library-root "${LIBRARY_ROOT}" \
  --force

"${ROOT_DIR}/target/release/legatofs" smoke \
  --config "${CLIENT_STATE}/legatofs.toml" \
  --path "${SAMPLE_PATH}" \
  --offset 6 \
  --size 6

echo "release validation smoke passed"

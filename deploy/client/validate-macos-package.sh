#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PKG_PATH="${1:-}"

if [[ -z "${PKG_PATH}" ]]; then
  echo "usage: $0 <pkg-path>" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
BUNDLE_DIR="${WORK_DIR}/bundle"
STATE_DIR="${WORK_DIR}/state"

cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${BUNDLE_DIR}" "${STATE_DIR}"

printf 'ca\n' > "${BUNDLE_DIR}/server-ca.pem"
printf 'client\n' > "${BUNDLE_DIR}/client.pem"
printf 'key\n' > "${BUNDLE_DIR}/client-key.pem"
cat > "${BUNDLE_DIR}/bundle.json" <<'EOF'
{
  "client_name": "release-macos",
  "endpoint": "legato.lan:7823",
  "server_name": "legato.lan",
  "mount_point": "/Volumes/Legato",
  "library_root": "/srv/libraries",
  "issued_at_unix_ms": 1
}
EOF

sudo installer -pkg "${PKG_PATH}" -target /

/usr/local/bin/legato-register-client \
  --bundle-dir "${BUNDLE_DIR}" \
  --state-dir "${STATE_DIR}" \
  --force

test -x /usr/local/bin/legatofs
test -x /usr/local/bin/legato-register-client
test -f "${STATE_DIR}/legatofs.toml"
test -d "${STATE_DIR}/certs"
test -d "${STATE_DIR}/extents"
test -f "${STATE_DIR}/certs/server-ca.pem"
test -f "${STATE_DIR}/certs/client.pem"
test -f "${STATE_DIR}/certs/client-key.pem"
grep -q 'endpoint = "legato.lan:7823"' "${STATE_DIR}/legatofs.toml"
grep -q 'server_name = "legato.lan"' "${STATE_DIR}/legatofs.toml"

echo "macOS package validation passed"

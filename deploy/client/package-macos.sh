#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${1:-${ROOT_DIR}/artifacts/macos}"
VERSION="${VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' "${ROOT_DIR}/Cargo.toml" | head -n1)}"
STATE_DIR="/Library/Application Support/Legato"
CERT_DIR="${STATE_DIR}/certs"
MOUNT_POINT="${LEGATO_MACOS_MOUNT_POINT:-/Volumes/Legato}"

PACKAGE_ID="io.legato.legatofs"
PACKAGE_NAME="legatofs-${VERSION}-macos.pkg"
BUILD_DIR="$(mktemp -d)"
STAGE_DIR="${BUILD_DIR}/root"
SCRIPTS_DIR="${BUILD_DIR}/scripts"

cleanup() {
  rm -rf "${BUILD_DIR}"
}
trap cleanup EXIT

mkdir -p "${OUTPUT_DIR}" "${STAGE_DIR}/usr/local/bin" "${STAGE_DIR}${STATE_DIR}" "${STAGE_DIR}${CERT_DIR}" "${SCRIPTS_DIR}"

cp "${ROOT_DIR}/target/release/legatofs" "${STAGE_DIR}/usr/local/bin/legatofs"
cp "${ROOT_DIR}/deploy/client/macos/register-client.sh" "${STAGE_DIR}/usr/local/bin/legato-register-client"
cp "${ROOT_DIR}/deploy/client/macos/setup-client.sh" "${STAGE_DIR}/usr/local/bin/legato-setup-client"
sed \
  -e "s#__CERT_DIR__#${CERT_DIR//\\/\\\\}#g" \
  -e "s#__MOUNT_POINT__#${MOUNT_POINT//\\/\\\\}#g" \
  -e "s#__STATE_DIR__#${STATE_DIR//\\/\\\\}#g" \
  "${ROOT_DIR}/deploy/client/config/legatofs.toml.example" \
  > "${STAGE_DIR}${STATE_DIR}/legatofs.toml.example"
cp "${ROOT_DIR}/deploy/client/config/certs-README.txt" "${STAGE_DIR}${STATE_DIR}/certs-README.txt"
cp "${ROOT_DIR}/deploy/client/macos/scripts/preinstall" "${SCRIPTS_DIR}/preinstall"
cp "${ROOT_DIR}/deploy/client/macos/scripts/postinstall" "${SCRIPTS_DIR}/postinstall"
chmod 755 "${SCRIPTS_DIR}/preinstall" "${SCRIPTS_DIR}/postinstall"
chmod 755 \
  "${STAGE_DIR}/usr/local/bin/legatofs" \
  "${STAGE_DIR}/usr/local/bin/legato-register-client" \
  "${STAGE_DIR}/usr/local/bin/legato-setup-client"

pkgbuild \
  --root "${STAGE_DIR}" \
  --identifier "${PACKAGE_ID}" \
  --version "${VERSION}" \
  --scripts "${SCRIPTS_DIR}" \
  "${OUTPUT_DIR}/${PACKAGE_NAME}"

echo "created ${OUTPUT_DIR}/${PACKAGE_NAME}"

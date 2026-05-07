#!/bin/bash
set -euo pipefail

# Applies ownership and SMB-friendly passthrough settings to the existing
# Legato library share root and the expected child folders.
#
# This script does not create datasets, SMB shares, CIFS mounts, or Docker
# mounts. It only adjusts permissions on:
#   /mnt/apps/shares/legato
#   /mnt/apps/shares/legato/VST
#   /mnt/apps/shares/legato/samples
#   /mnt/apps/shares/legato/kontakt

LEGATO_SHARE_ROOT="${LEGATO_SHARE_ROOT:-/mnt/apps/shares/legato}"
LEGATO_USER="${LEGATO_USER:-legato}"
LEGATO_GROUP="${LEGATO_GROUP:-legato}"
LEGATO_CHILDREN="${LEGATO_CHILDREN:-VST samples kontakt}"
LEGATO_RECURSIVE="${LEGATO_RECURSIVE:-false}"
LEGATO_SET_ZFS_PROPERTIES="${LEGATO_SET_ZFS_PROPERTIES:-true}"
LEGATO_DIR_MODE="${LEGATO_DIR_MODE:-2770}"
LEGATO_FILE_MODE="${LEGATO_FILE_MODE:-660}"

require_root() {
  if [ "${EUID}" -ne 0 ]; then
    echo "error: this script must run as root" >&2
    exit 1
  fi
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "error: required command not found: ${command_name}" >&2
    exit 1
  fi
}

require_existing_directory() {
  local path="$1"
  if [ ! -d "${path}" ]; then
    echo "error: expected directory does not exist: ${path}" >&2
    exit 1
  fi
}

dataset_for_mountpoint() {
  local path="$1"
  zfs list -H -o name,mountpoint | awk -v target="${path}" '$2 == target { print $1; exit }'
}

apply_zfs_passthrough_properties() {
  local path="$1"
  local dataset
  dataset="$(dataset_for_mountpoint "${path}")"

  if [ -z "${dataset}" ]; then
    echo "warn: no ZFS dataset has mountpoint ${path}; skipping ZFS ACL properties" >&2
    return
  fi

  echo "setting SMB passthrough ZFS properties on ${dataset}"
  zfs set acltype=nfsv4 "${dataset}"
  zfs set aclmode=passthrough "${dataset}"
  zfs set aclinherit=passthrough "${dataset}"
  zfs set xattr=sa "${dataset}"
}

apply_owner_and_modes() {
  local path="$1"

  echo "setting owner and directory mode on ${path}"
  chown "${LEGATO_USER}:${LEGATO_GROUP}" "${path}"
  chmod "${LEGATO_DIR_MODE}" "${path}"

  if [ "${LEGATO_RECURSIVE}" = "true" ]; then
    echo "recursively setting owner and modes under ${path}"
    chown -R "${LEGATO_USER}:${LEGATO_GROUP}" "${path}"
    find "${path}" -type d -exec chmod "${LEGATO_DIR_MODE}" {} +
    find "${path}" -type f -exec chmod "${LEGATO_FILE_MODE}" {} +
  fi
}

main() {
  require_root
  require_command chown
  require_command chmod
  require_command find

  if [ "${LEGATO_SET_ZFS_PROPERTIES}" = "true" ]; then
    require_command zfs
  fi

  require_existing_directory "${LEGATO_SHARE_ROOT}"

  local paths=("${LEGATO_SHARE_ROOT}")
  local child
  for child in ${LEGATO_CHILDREN}; do
    paths+=("${LEGATO_SHARE_ROOT}/${child}")
  done

  local path
  for path in "${paths[@]}"; do
    require_existing_directory "${path}"
    if [ "${LEGATO_SET_ZFS_PROPERTIES}" = "true" ]; then
      apply_zfs_passthrough_properties "${path}"
    fi
    apply_owner_and_modes "${path}"
  done

  cat <<EOF
Legato share permissions applied.

Share root:
  ${LEGATO_SHARE_ROOT}

Expected children:
  ${LEGATO_CHILDREN}

Owner:
  ${LEGATO_USER}:${LEGATO_GROUP}

Modes:
  directories ${LEGATO_DIR_MODE}
  files       ${LEGATO_FILE_MODE} when LEGATO_RECURSIVE=true

Recursive:
  ${LEGATO_RECURSIVE}
EOF
}

main "$@"

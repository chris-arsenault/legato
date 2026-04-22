#!/usr/bin/env bash
set -euo pipefail

# Creates the Legato datasets and local account layout on a TrueNAS host.
#
# Default dataset layout:
#   /mnt/apps/apps/legato
#   /mnt/apps/apps/legato/config
#   /mnt/apps/shares/legato/VST
#   /mnt/apps/shares/legato/samples
#   /mnt/apps/shares/legato/kontakt
#
# The three share datasets are prepared for SMB export with NFSv4 ACL
# passthrough semantics. This script does not create SMB share objects in
# TrueNAS; it prepares the datasets and permissions so they can be shared
# cleanly afterward.

POOL="${POOL:-apps}"
APPS_ROOT_DATASET="${APPS_ROOT_DATASET:-${POOL}/apps}"
SHARES_ROOT_DATASET="${SHARES_ROOT_DATASET:-${POOL}/shares}"

LEGATO_USER="${LEGATO_USER:-legato}"
LEGATO_GROUP="${LEGATO_GROUP:-legato}"
LEGATO_UID="${LEGATO_UID:-10001}"
LEGATO_GID="${LEGATO_GID:-10001}"

APP_DATASET="${APP_DATASET:-${APPS_ROOT_DATASET}/legato}"
APP_CONFIG_DATASET="${APP_CONFIG_DATASET:-${APP_DATASET}/config}"

VST_DATASET="${VST_DATASET:-${SHARES_ROOT_DATASET}/legato/VST}"
SAMPLES_DATASET="${SAMPLES_DATASET:-${SHARES_ROOT_DATASET}/legato/samples}"
KONTAKT_DATASET="${KONTAKT_DATASET:-${SHARES_ROOT_DATASET}/legato/kontakt}"

SHARE_DATASETS=(
  "${VST_DATASET}"
  "${SAMPLES_DATASET}"
  "${KONTAKT_DATASET}"
)

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "error: required command not found: ${command_name}" >&2
    exit 1
  fi
}

dataset_mountpoint() {
  zfs get -H -o value mountpoint "$1"
}

dataset_exists() {
  zfs list -H -o name "$1" >/dev/null 2>&1
}

create_dataset_if_missing() {
  local dataset="$1"
  shift

  if dataset_exists "${dataset}"; then
    echo "dataset exists: ${dataset}"
    return
  fi

  echo "creating dataset: ${dataset}"
  zfs create -p "$@" "${dataset}"
}

ensure_group() {
  if getent group "${LEGATO_GROUP}" >/dev/null 2>&1; then
    echo "group exists: ${LEGATO_GROUP}"
    return
  fi

  echo "creating group: ${LEGATO_GROUP} (${LEGATO_GID})"
  if command -v midclt >/dev/null 2>&1; then
    midclt call group.create "{\"gid\": ${LEGATO_GID}, \"name\": \"${LEGATO_GROUP}\", \"smb\": true}" >/dev/null
  else
    groupadd -g "${LEGATO_GID}" "${LEGATO_GROUP}"
  fi
}

ensure_user() {
  if id -u "${LEGATO_USER}" >/dev/null 2>&1; then
    echo "user exists: ${LEGATO_USER}"
    return
  fi

  echo "creating user: ${LEGATO_USER} (${LEGATO_UID})"
  if command -v midclt >/dev/null 2>&1; then
    midclt call user.create "$(cat <<EOF
{
  "uid": ${LEGATO_UID},
  "username": "${LEGATO_USER}",
  "group_create": false,
  "group": $(getent group "${LEGATO_GROUP}" | cut -d: -f3),
  "home": "/nonexistent",
  "shell": "/usr/sbin/nologin",
  "full_name": "Legato Service User",
  "password_disabled": true,
  "smb": true
}
EOF
)" >/dev/null
  else
    useradd \
      -u "${LEGATO_UID}" \
      -g "${LEGATO_GROUP}" \
      -d /nonexistent \
      -s /usr/sbin/nologin \
      -M \
      "${LEGATO_USER}"
  fi
}

apply_share_acl_properties() {
  local dataset="$1"
  echo "setting NFSv4 passthrough ACL properties on ${dataset}"
  zfs set acltype=nfsv4 "${dataset}"
  zfs set aclmode=passthrough "${dataset}"
  zfs set aclinherit=passthrough "${dataset}"
  zfs set xattr=sa "${dataset}"
}

apply_owner_and_mode() {
  local path="$1"
  local mode="$2"

  echo "setting owner/group on ${path}"
  chown "${LEGATO_USER}:${LEGATO_GROUP}" "${path}"
  chmod "${mode}" "${path}"
}

main() {
  require_command zfs
  require_command getent
  require_command chown
  require_command chmod

  ensure_group
  ensure_user

  create_dataset_if_missing "${APP_DATASET}"
  create_dataset_if_missing "${APP_CONFIG_DATASET}"

  for dataset in "${SHARE_DATASETS[@]}"; do
    create_dataset_if_missing "${dataset}"
    apply_share_acl_properties "${dataset}"
  done

  apply_owner_and_mode "$(dataset_mountpoint "${APP_DATASET}")" 770
  apply_owner_and_mode "$(dataset_mountpoint "${APP_CONFIG_DATASET}")" 770

  for dataset in "${SHARE_DATASETS[@]}"; do
    apply_owner_and_mode "$(dataset_mountpoint "${dataset}")" 770
  done

  cat <<EOF

Legato datasets are ready.

App datasets:
  $(dataset_mountpoint "${APP_DATASET}")
  $(dataset_mountpoint "${APP_CONFIG_DATASET}")

Share datasets:
  $(dataset_mountpoint "${VST_DATASET}")
  $(dataset_mountpoint "${SAMPLES_DATASET}")
  $(dataset_mountpoint "${KONTAKT_DATASET}")

Next step:
  Create SMB share definitions in TrueNAS for the three share datasets if you
  want them exported over SMB.
EOF
}

main "$@"

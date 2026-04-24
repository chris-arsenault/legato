#!/bin/bash
set -euo pipefail

LEGATOFS="/usr/local/bin/legatofs"

prompt() {
  local message="$1"
  local default_value="$2"
  /usr/bin/osascript <<OSA
set answer to text returned of (display dialog "${message}" default answer "${default_value}" buttons {"Continue"} default button "Continue")
return answer
OSA
}

client_name_default="$(/usr/sbin/scutil --get LocalHostName 2>/dev/null || /bin/hostname)"
bootstrap_url="$(prompt "Legato server bootstrap URL. Leave blank to discover the server on this LAN." "")"
client_name="$(prompt "Legato client name." "${client_name_default}")"
mount_point="$(prompt "Legato mount point." "/Volumes/Legato")"

install_args=(
  install
  --client-name "${client_name}"
  --mount-point "${mount_point}"
  --state-dir "/Library/Application Support/Legato"
  --library-root "/"
  --force
)
if [[ -n "${bootstrap_url}" ]]; then
  install_args+=(--bootstrap-url "${bootstrap_url}")
fi

/usr/bin/sudo "${LEGATOFS}" "${install_args[@]}"
"${LEGATOFS}" service install --force
"${LEGATOFS}" service start

/usr/bin/osascript <<OSA
display dialog "Legato client setup is complete. Mount point: ${mount_point}" buttons {"OK"} default button "OK"
OSA

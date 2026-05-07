# Legato SMB Setup

These are one-time operational commands for the TrueNAS `shares/legato` dataset and temporary host SMB mounts used to copy library data into the share.

Assumptions:

- TrueNAS host IP: `192.168.66.3`
- SMB share name: `legato`
- Share root: `/mnt/apps/shares/legato`
- Legato UID/GID: `42173`
- Legato SMB user: `legato`
- Child datasets/folders: `VST`, `samples`, `kontakt`

## TrueNAS Permissions

Run this in the TrueNAS shell as `root`.

```bash
ROOT=/mnt/apps/shares/legato
LEGATO_UID=42173
LEGATO_GID=42173

for p in "$ROOT" "$ROOT/VST" "$ROOT/samples" "$ROOT/kontakt"; do
  ds="$(zfs list -H -o name,mountpoint | awk -v p="$p" '$2 == p { print $1; exit }')"
  test -n "$ds" || { echo "no dataset mounted exactly at: $p"; exit 1; }

  echo "setting NFSv4 ACL dataset mode on $ds"
  midclt call pool.dataset.update "$ds" '{"acltype":"NFSV4","aclmode":"RESTRICTED"}'

  echo "applying NFSv4 ACL to $p"
  JOB=$(midclt call filesystem.setacl "{\"path\":\"$p\",\"uid\":$LEGATO_UID,\"gid\":$LEGATO_GID,\"acltype\":\"NFS4\",\"dacl\":[{\"tag\":\"owner@\",\"type\":\"ALLOW\",\"perms\":{\"BASIC\":\"FULL_CONTROL\"},\"flags\":{\"BASIC\":\"INHERIT\"}},{\"tag\":\"GROUP\",\"id\":$LEGATO_GID,\"type\":\"ALLOW\",\"perms\":{\"BASIC\":\"MODIFY\"},\"flags\":{\"BASIC\":\"INHERIT\"}},{\"tag\":\"everyone@\",\"type\":\"ALLOW\",\"perms\":{\"BASIC\":\"TRAVERSE\"},\"flags\":{\"BASIC\":\"INHERIT\"}}],\"nfs41_flags\":{\"autoinherit\":true,\"protected\":false,\"defaulted\":false},\"options\":{\"recursive\":false,\"traverse\":false,\"validate_effective_acl\":false}}")
  midclt call core.job_wait "$JOB"
done
```

Verify that each path reports `acltype: NFS4` and `trivial: false`.

```bash
for p in /mnt/apps/shares/legato /mnt/apps/shares/legato/VST /mnt/apps/shares/legato/samples /mnt/apps/shares/legato/kontakt; do
  echo "$p"
  midclt call filesystem.getacl "$p"
done
```

## Windows SMB Mount

Run this from PowerShell on the Windows host. Replace `YOUR_PASSWORD_HERE`.

```powershell
net use K: /delete /y
net use K: "\\192.168.66.3\legato" /user:legato "YOUR_PASSWORD_HERE" /persistent:yes
```

If TrueNAS requires a server-qualified user, use this form instead.

```powershell
net use K: /delete /y
net use K: "\\192.168.66.3\legato" /user:192.168.66.3\legato "YOUR_PASSWORD_HERE" /persistent:yes
```

Verify the mount.

```powershell
net use
dir K:\
```

`net use` persists the drive for the current Windows user. A true all-users mapping requires Windows global SMB mapping support, which is more fragile with stored credentials.

## macOS SMB Mount

Create the mount point.

```bash
sudo mkdir -p /Volumes/Legato
```

Mount with a password prompt.

```bash
mount_smbfs '//legato@192.168.66.3/legato' /Volumes/Legato
```

Or mount with the password inline.

```bash
mount_smbfs '//legato:YOUR_PASSWORD_HERE@192.168.66.3/legato' /Volumes/Legato
```

Verify the mount.

```bash
ls -la /Volumes/Legato
mount | grep Legato
```

For a persistent user mount, edit `/etc/fstab`.

```bash
sudo vifs
```

Add this line, replacing the password.

```fstab
//legato:YOUR_PASSWORD_HERE@192.168.66.3/legato /Volumes/Legato smbfs rw,noauto 0 0
```

Mount it later with:

```bash
mount /Volumes/Legato
```

//! Library reconciliation and stable file-ID persistence for the server metadata DB.

use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use legato_proto::TransferClass;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use walkdir::WalkDir;

use crate::{LayoutPolicy, is_policy_path};

/// Summary of changes observed during a reconciliation run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileStats {
    /// Number of directories created during the scan.
    pub directories_created: u64,
    /// Number of files created during the scan.
    pub files_created: u64,
    /// Number of existing directories updated in place.
    pub directories_updated: u64,
    /// Number of existing files updated in place.
    pub files_updated: u64,
    /// Number of directories removed because they no longer exist on disk.
    pub directories_deleted: u64,
    /// Number of files removed because they no longer exist on disk.
    pub files_deleted: u64,
}

/// Reconciles the library root on disk with the persistent metadata index.
pub fn reconcile_library_root(
    connection: &mut Connection,
    library_root: &Path,
) -> rusqlite::Result<ReconcileStats> {
    reconcile_paths(connection, library_root, &[])
}

/// Reconciles a targeted set of paths, or the whole tree when `paths` is empty.
pub fn reconcile_paths(
    connection: &mut Connection,
    library_root: &Path,
    paths: &[PathBuf],
) -> rusqlite::Result<ReconcileStats> {
    let layout_policy = LayoutPolicy::load(library_root).map_err(|error| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            error.to_string(),
        )))
    })?;
    let root = fs::canonicalize(library_root)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let transaction = connection.transaction()?;

    let mut stats = ReconcileStats::default();

    let policy_changed = paths.iter().any(|path| {
        let candidate = if path.is_absolute() {
            path.clone()
        } else {
            root.join(path)
        };
        is_policy_path(&root, &candidate)
    });

    let scope_roots = if paths.is_empty() || policy_changed {
        vec![root.clone()]
    } else {
        normalize_scope_roots(&root, paths)?
    };

    for scope_root in &scope_roots {
        reconcile_scope(&transaction, &root, scope_root, &layout_policy, &mut stats)?;
    }

    transaction.commit()?;
    Ok(stats)
}

#[derive(Clone, Copy, Debug, Default)]
struct FilesystemIdentity {
    device_id: i64,
    inode: i64,
}

#[derive(Clone, Copy, Debug)]
struct FileObservation<'a> {
    directory_id: i64,
    path: &'a str,
    size: u64,
    mtime_ns: i64,
    device_id: i64,
    inode: i64,
    transfer_class: TransferClass,
    extent_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UpsertResult {
    record_id: i64,
    changed: bool,
}

#[derive(Clone, Debug)]
struct ChangeRecordInput<'a> {
    kind: legato_proto::ChangeKind,
    file_id: i64,
    path: &'a str,
    is_dir: bool,
    size: i64,
    mtime_ns: i64,
    transfer_class: Option<i64>,
    extent_bytes: Option<i64>,
}

#[cfg(target_family = "unix")]
fn filesystem_identity(metadata: &fs::Metadata) -> FilesystemIdentity {
    use std::os::unix::fs::MetadataExt;

    FilesystemIdentity {
        device_id: metadata.dev() as i64,
        inode: metadata.ino() as i64,
    }
}

#[cfg(not(target_family = "unix"))]
fn filesystem_identity(_metadata: &fs::Metadata) -> FilesystemIdentity {
    FilesystemIdentity::default()
}

fn modified_time_ns(metadata: &fs::Metadata) -> rusqlite::Result<i64> {
    let modified = metadata
        .modified()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    Ok(duration.as_nanos() as i64)
}

fn current_time_ns() -> rusqlite::Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
        .as_nanos() as i64)
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn normalize_scope_roots(root: &Path, paths: &[PathBuf]) -> rusqlite::Result<Vec<PathBuf>> {
    let mut normalized = Vec::new();

    for path in paths {
        let candidate = if path.is_absolute() {
            path.clone()
        } else {
            root.join(path)
        };

        let scoped = if candidate.exists() {
            fs::canonicalize(&candidate)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
        } else {
            candidate
        };

        if scoped == *root || scoped.starts_with(root) {
            normalized.push(scoped);
        }
    }

    normalized.sort();
    normalized.dedup();

    let mut filtered = Vec::new();
    'outer: for candidate in normalized {
        for existing in &filtered {
            if candidate.starts_with(existing) {
                continue 'outer;
            }
        }
        filtered.push(candidate);
    }

    Ok(filtered)
}

fn reconcile_scope(
    transaction: &Transaction<'_>,
    library_root: &Path,
    scope_root: &Path,
    layout_policy: &LayoutPolicy,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    if scope_root.exists() {
        let metadata = fs::metadata(scope_root)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        if metadata.is_dir() {
            reconcile_existing_directory(
                transaction,
                library_root,
                scope_root,
                layout_policy,
                stats,
            )?;
        } else if metadata.is_file() {
            if is_policy_path(library_root, scope_root) {
                reconcile_existing_directory(
                    transaction,
                    library_root,
                    library_root,
                    layout_policy,
                    stats,
                )?;
            } else {
                reconcile_existing_file(
                    transaction,
                    library_root,
                    scope_root,
                    layout_policy,
                    stats,
                )?;
            }
        }
    } else {
        prune_missing_scope(transaction, scope_root, library_root, stats)?;
        return Ok(());
    }

    let mut seen_directories = HashSet::new();
    let mut seen_files = HashSet::new();

    for entry in WalkDir::new(scope_root).sort_by_file_name() {
        let entry =
            entry.map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let path_string = normalize_path(entry.path());

        if entry.file_type().is_dir() {
            seen_directories.insert(path_string);
        } else if entry.file_type().is_file() && !is_policy_path(library_root, entry.path()) {
            seen_files.insert(path_string);
        }
    }

    prune_missing_files(transaction, scope_root, &seen_files, stats)?;
    prune_missing_directories(transaction, scope_root, &seen_directories, stats)?;
    Ok(())
}

fn reconcile_existing_directory(
    transaction: &Transaction<'_>,
    library_root: &Path,
    directory_root: &Path,
    layout_policy: &LayoutPolicy,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    ensure_directory_chain(transaction, library_root, directory_root, stats)?;

    for entry in WalkDir::new(directory_root).sort_by_file_name() {
        let entry =
            entry.map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let metadata = entry
            .metadata()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let path = entry.path();
        let path_string = normalize_path(path);
        let identity = filesystem_identity(&metadata);
        let mtime_ns = modified_time_ns(&metadata)?;

        if metadata.is_dir() {
            let parent_path = path
                .parent()
                .filter(|parent| *parent != path)
                .map(normalize_path);
            let upsert = upsert_directory(
                transaction,
                &path_string,
                parent_path.as_deref(),
                identity.device_id,
                identity.inode,
                mtime_ns,
                stats,
            )?;
            if upsert.changed {
                record_change(
                    transaction,
                    ChangeRecordInput {
                        kind: legato_proto::ChangeKind::Upsert,
                        file_id: upsert.record_id,
                        path: &path_string,
                        is_dir: true,
                        size: 0,
                        mtime_ns,
                        transfer_class: None,
                        extent_bytes: None,
                    },
                )?;
            }
        } else if metadata.is_file() {
            if is_policy_path(library_root, path) {
                continue;
            }
            let directory_path = normalize_path(
                path.parent()
                    .expect("walked file entries always have a parent directory"),
            );
            let directory_id = lookup_directory_id(transaction, &directory_path)?
                .ok_or_else(|| rusqlite::Error::InvalidParameterName(directory_path.clone()))?;
            let decision = layout_policy.file_decision(&path_string, metadata.len(), false);
            let upsert = upsert_file(
                transaction,
                FileObservation {
                    directory_id,
                    path: &path_string,
                    size: metadata.len(),
                    mtime_ns,
                    device_id: identity.device_id,
                    inode: identity.inode,
                    transfer_class: decision.transfer_class,
                    extent_bytes: decision.stored_extent_bytes(metadata.len(), false),
                },
                stats,
            )?;
            if upsert.changed {
                record_change(
                    transaction,
                    ChangeRecordInput {
                        kind: legato_proto::ChangeKind::Upsert,
                        file_id: upsert.record_id,
                        path: &path_string,
                        is_dir: false,
                        size: metadata.len() as i64,
                        mtime_ns,
                        transfer_class: Some(decision.transfer_class as i64),
                        extent_bytes: Some(
                            decision.stored_extent_bytes(metadata.len(), false) as i64
                        ),
                    },
                )?;
            }
        }
    }

    Ok(())
}

fn reconcile_existing_file(
    transaction: &Transaction<'_>,
    library_root: &Path,
    file_path: &Path,
    layout_policy: &LayoutPolicy,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    if is_policy_path(library_root, file_path) {
        return reconcile_existing_directory(
            transaction,
            library_root,
            library_root,
            layout_policy,
            stats,
        );
    }

    let parent = file_path
        .parent()
        .expect("reconciled file paths always have a parent directory");
    ensure_directory_chain(transaction, library_root, parent, stats)?;

    let metadata = fs::metadata(file_path)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let identity = filesystem_identity(&metadata);
    let mtime_ns = modified_time_ns(&metadata)?;
    let file_string = normalize_path(file_path);
    let parent_string = normalize_path(parent);
    let directory_id = lookup_directory_id(transaction, &parent_string)?
        .ok_or_else(|| rusqlite::Error::InvalidParameterName(parent_string.clone()))?;
    let decision = layout_policy.file_decision(&file_string, metadata.len(), false);

    let upsert = upsert_file(
        transaction,
        FileObservation {
            directory_id,
            path: &file_string,
            size: metadata.len(),
            mtime_ns,
            device_id: identity.device_id,
            inode: identity.inode,
            transfer_class: decision.transfer_class,
            extent_bytes: decision.stored_extent_bytes(metadata.len(), false),
        },
        stats,
    )?;
    if upsert.changed {
        record_change(
            transaction,
            ChangeRecordInput {
                kind: legato_proto::ChangeKind::Upsert,
                file_id: upsert.record_id,
                path: &file_string,
                is_dir: false,
                size: metadata.len() as i64,
                mtime_ns,
                transfer_class: Some(decision.transfer_class as i64),
                extent_bytes: Some(decision.stored_extent_bytes(metadata.len(), false) as i64),
            },
        )?;
    }
    Ok(())
}

fn ensure_directory_chain(
    transaction: &Transaction<'_>,
    library_root: &Path,
    directory_path: &Path,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    let mut chain = directory_path
        .ancestors()
        .take_while(|path| path.starts_with(library_root))
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    chain.reverse();

    for path in chain {
        if !path.exists() {
            continue;
        }
        let metadata = fs::metadata(&path)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        if !metadata.is_dir() {
            continue;
        }
        let identity = filesystem_identity(&metadata);
        let mtime_ns = modified_time_ns(&metadata)?;
        let path_string = normalize_path(&path);
        let parent_path = path
            .parent()
            .filter(|parent| *parent != path)
            .map(normalize_path);
        let upsert = upsert_directory(
            transaction,
            &path_string,
            parent_path.as_deref(),
            identity.device_id,
            identity.inode,
            mtime_ns,
            stats,
        )?;
        if upsert.changed {
            record_change(
                transaction,
                ChangeRecordInput {
                    kind: legato_proto::ChangeKind::Upsert,
                    file_id: upsert.record_id,
                    path: &path_string,
                    is_dir: true,
                    size: 0,
                    mtime_ns,
                    transfer_class: None,
                    extent_bytes: None,
                },
            )?;
        }
    }

    Ok(())
}

fn prune_missing_scope(
    transaction: &Transaction<'_>,
    scope_root: &Path,
    library_root: &Path,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    let scope_string = normalize_path(scope_root);
    if scope_root == library_root {
        record_deleted_paths(
            transaction,
            "SELECT file_id, path FROM files",
            legato_proto::ChangeKind::Delete,
            false,
        )?;
        record_deleted_paths(
            transaction,
            "SELECT directory_id, path FROM directories",
            legato_proto::ChangeKind::Delete,
            true,
        )?;
        let deleted_files = transaction.execute("DELETE FROM files", [])?;
        let deleted_dirs = transaction.execute("DELETE FROM directories", [])?;
        stats.files_deleted += deleted_files as u64;
        stats.directories_deleted += deleted_dirs as u64;
        return Ok(());
    }

    let prefix = format!("{scope_string}/%");
    record_deleted_scope(
        transaction,
        "SELECT file_id, path FROM files WHERE path = ?1 OR path LIKE ?2",
        &scope_string,
        legato_proto::ChangeKind::Delete,
        false,
    )?;
    stats.files_deleted += transaction.execute(
        "DELETE FROM files WHERE path = ?1 OR path LIKE ?2",
        params![scope_string, prefix],
    )? as u64;
    record_deleted_scope(
        transaction,
        "SELECT directory_id, path FROM directories WHERE path = ?1 OR path LIKE ?2",
        &scope_string,
        legato_proto::ChangeKind::Delete,
        true,
    )?;
    stats.directories_deleted += transaction.execute(
        "DELETE FROM directories WHERE path = ?1 OR path LIKE ?2",
        params![scope_string, prefix],
    )? as u64;
    Ok(())
}

fn lookup_directory_id(transaction: &Transaction<'_>, path: &str) -> rusqlite::Result<Option<i64>> {
    transaction
        .query_row(
            "SELECT directory_id FROM directories WHERE path = ?1",
            [path],
            |row| row.get(0),
        )
        .optional()
}

fn upsert_directory(
    transaction: &Transaction<'_>,
    path: &str,
    parent_path: Option<&str>,
    device_id: i64,
    inode: i64,
    mtime_ns: i64,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<UpsertResult> {
    let parent_directory_id = match parent_path {
        Some(parent_path) => lookup_directory_id(transaction, parent_path)?,
        None => None,
    };

    if let Some((
        directory_id,
        current_parent_id,
        current_device_id,
        current_inode,
        current_mtime_ns,
    )) = transaction
        .query_row(
            "SELECT directory_id, parent_directory_id, device_id, inode, mtime_ns
             FROM directories
             WHERE path = ?1",
            [path],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?
    {
        if current_parent_id == parent_directory_id
            && current_device_id == device_id
            && current_inode == inode
            && current_mtime_ns == mtime_ns
        {
            return Ok(UpsertResult {
                record_id: directory_id,
                changed: false,
            });
        }
        transaction.execute(
            "UPDATE directories
             SET parent_directory_id = ?2, device_id = ?3, inode = ?4, mtime_ns = ?5
             WHERE directory_id = ?1",
            params![
                directory_id,
                parent_directory_id,
                device_id,
                inode,
                mtime_ns
            ],
        )?;
        stats.directories_updated += 1;
        return Ok(UpsertResult {
            record_id: directory_id,
            changed: true,
        });
    }

    if let Some(directory_id) = transaction
        .query_row(
            "SELECT directory_id FROM directories WHERE device_id = ?1 AND inode = ?2",
            params![device_id, inode],
            |row| row.get(0),
        )
        .optional()?
    {
        transaction.execute(
            "UPDATE directories
             SET path = ?2, parent_directory_id = ?3, mtime_ns = ?4
             WHERE directory_id = ?1",
            params![directory_id, path, parent_directory_id, mtime_ns],
        )?;
        stats.directories_updated += 1;
        return Ok(UpsertResult {
            record_id: directory_id,
            changed: true,
        });
    }

    transaction.execute(
        "INSERT INTO directories (path, parent_directory_id, device_id, inode, mtime_ns)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![path, parent_directory_id, device_id, inode, mtime_ns],
    )?;
    stats.directories_created += 1;
    Ok(UpsertResult {
        record_id: transaction.last_insert_rowid(),
        changed: true,
    })
}

fn upsert_file(
    transaction: &Transaction<'_>,
    observation: FileObservation<'_>,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<UpsertResult> {
    if let Some((
        file_id,
        current_directory_id,
        current_size,
        current_mtime_ns,
        current_device_id,
        current_inode,
        current_transfer_class,
        current_extent_bytes,
    )) = transaction
        .query_row(
            "SELECT file_id, directory_id, size, mtime_ns, device_id, inode, transfer_class, extent_bytes
             FROM files WHERE path = ?1",
            [observation.path],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?
    {
        if current_directory_id == observation.directory_id
            && current_size == observation.size as i64
            && current_mtime_ns == observation.mtime_ns
            && current_device_id == observation.device_id
            && current_inode == observation.inode
            && current_transfer_class == observation.transfer_class as i64
            && current_extent_bytes == observation.extent_bytes as i64
        {
            return Ok(UpsertResult {
                record_id: file_id,
                changed: false,
            });
        }
        transaction.execute(
            "UPDATE files
             SET directory_id = ?2, size = ?3, mtime_ns = ?4, device_id = ?5, inode = ?6,
                 transfer_class = ?7, extent_bytes = ?8, updated_at_ns = ?4
             WHERE file_id = ?1",
            params![
                file_id,
                observation.directory_id,
                observation.size as i64,
                observation.mtime_ns,
                observation.device_id,
                observation.inode,
                observation.transfer_class as i64,
                observation.extent_bytes as i64
            ],
        )?;
        stats.files_updated += 1;
        return Ok(UpsertResult {
            record_id: file_id,
            changed: true,
        });
    }

    if let Some(file_id) = transaction
        .query_row(
            "SELECT file_id FROM files WHERE device_id = ?1 AND inode = ?2",
            params![observation.device_id, observation.inode],
            |row| row.get(0),
        )
        .optional()?
    {
        transaction.execute(
            "UPDATE files
             SET directory_id = ?2, path = ?3, size = ?4, mtime_ns = ?5,
                 transfer_class = ?6, extent_bytes = ?7, updated_at_ns = ?5
             WHERE file_id = ?1",
            params![
                file_id,
                observation.directory_id,
                observation.path,
                observation.size as i64,
                observation.mtime_ns,
                observation.transfer_class as i64,
                observation.extent_bytes as i64
            ],
        )?;
        stats.files_updated += 1;
        return Ok(UpsertResult {
            record_id: file_id,
            changed: true,
        });
    }

    transaction.execute(
        "INSERT INTO files (
             directory_id, path, size, mtime_ns, device_id, inode, transfer_class, extent_bytes,
             created_at_ns, updated_at_ns
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?4, ?4)",
        params![
            observation.directory_id,
            observation.path,
            observation.size as i64,
            observation.mtime_ns,
            observation.device_id,
            observation.inode,
            observation.transfer_class as i64,
            observation.extent_bytes as i64
        ],
    )?;
    stats.files_created += 1;
    Ok(UpsertResult {
        record_id: transaction.last_insert_rowid(),
        changed: true,
    })
}

fn prune_missing_files(
    transaction: &Transaction<'_>,
    root: &Path,
    seen_files: &HashSet<String>,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    let root_prefix = normalize_path(root);
    let existing_paths = collect_paths(
        transaction,
        "SELECT path FROM files WHERE path = ?1 OR path LIKE ?2",
        &root_prefix,
    )?;

    for path in existing_paths
        .into_iter()
        .filter(|path| !seen_files.contains(path))
    {
        if let Some(file_id) = transaction
            .query_row(
                "SELECT file_id FROM files WHERE path = ?1",
                [path.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            record_change(
                transaction,
                ChangeRecordInput {
                    kind: legato_proto::ChangeKind::Delete,
                    file_id,
                    path: &path,
                    is_dir: false,
                    size: 0,
                    mtime_ns: current_time_ns()?,
                    transfer_class: None,
                    extent_bytes: None,
                },
            )?;
        }
        stats.files_deleted +=
            transaction.execute("DELETE FROM files WHERE path = ?1", [path])? as u64;
    }

    Ok(())
}

fn prune_missing_directories(
    transaction: &Transaction<'_>,
    root: &Path,
    seen_directories: &HashSet<String>,
    stats: &mut ReconcileStats,
) -> rusqlite::Result<()> {
    let root_prefix = normalize_path(root);
    let mut existing_paths = collect_paths(
        transaction,
        "SELECT path FROM directories WHERE path = ?1 OR path LIKE ?2",
        &root_prefix,
    )?;
    existing_paths.sort_by_key(|path| std::cmp::Reverse(path.len()));

    for path in existing_paths
        .into_iter()
        .filter(|path| path != &root_prefix && !seen_directories.contains(path))
    {
        if let Some(directory_id) = transaction
            .query_row(
                "SELECT directory_id FROM directories WHERE path = ?1",
                [path.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            record_change(
                transaction,
                ChangeRecordInput {
                    kind: legato_proto::ChangeKind::Delete,
                    file_id: directory_id,
                    path: &path,
                    is_dir: true,
                    size: 0,
                    mtime_ns: current_time_ns()?,
                    transfer_class: None,
                    extent_bytes: None,
                },
            )?;
        }
        stats.directories_deleted +=
            transaction.execute("DELETE FROM directories WHERE path = ?1", [path])? as u64;
    }

    Ok(())
}

fn collect_paths(
    transaction: &Transaction<'_>,
    sql: &str,
    root_prefix: &str,
) -> rusqlite::Result<Vec<String>> {
    let like_prefix = format!("{root_prefix}/%");
    let mut statement = transaction.prepare(sql)?;
    statement
        .query_map(params![root_prefix, like_prefix], |row| {
            row.get::<_, String>(0)
        })?
        .collect()
}

fn record_change(
    transaction: &Transaction<'_>,
    change: ChangeRecordInput<'_>,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO change_log (
             kind, file_id, path, is_dir, size, mtime_ns, transfer_class, extent_bytes, recorded_at_ns
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            change.kind as i64,
            change.file_id,
            change.path,
            if change.is_dir { 1_i64 } else { 0_i64 },
            change.size,
            change.mtime_ns,
            change.transfer_class,
            change.extent_bytes,
            current_time_ns()?
        ],
    )?;
    Ok(())
}

fn record_deleted_scope(
    transaction: &Transaction<'_>,
    sql: &str,
    root_prefix: &str,
    kind: legato_proto::ChangeKind,
    is_dir: bool,
) -> rusqlite::Result<()> {
    let like_prefix = format!("{root_prefix}/%");
    let mut statement = transaction.prepare(sql)?;
    let rows = statement.query_map(params![root_prefix, like_prefix], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (file_id, path) = row?;
        record_change(
            transaction,
            ChangeRecordInput {
                kind,
                file_id,
                path: &path,
                is_dir,
                size: 0,
                mtime_ns: current_time_ns()?,
                transfer_class: None,
                extent_bytes: None,
            },
        )?;
    }
    Ok(())
}

fn record_deleted_paths(
    transaction: &Transaction<'_>,
    sql: &str,
    kind: legato_proto::ChangeKind,
    is_dir: bool,
) -> rusqlite::Result<()> {
    let mut statement = transaction.prepare(sql)?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (file_id, path) = row?;
        record_change(
            transaction,
            ChangeRecordInput {
                kind,
                file_id,
                path: &path,
                is_dir,
                size: 0,
                mtime_ns: current_time_ns()?,
                transfer_class: None,
                extent_bytes: None,
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, path::Path};

    use legato_proto::TransferClass;
    use rusqlite::{Connection, OptionalExtension};
    use tempfile::tempdir;

    use super::reconcile_library_root;
    use crate::open_metadata_database;

    #[test]
    fn reconcile_creates_directory_and_file_records() {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let mut file = fs::File::create(library_root.join("Kontakt").join("piano.nki"))
            .expect("fixture file should be created");
        writeln!(file, "fixture").expect("fixture file should be written");

        let db_dir = tempdir().expect("db tempdir should be created");
        let mut connection =
            open_metadata_database(&db_dir.path().join("server.sqlite")).expect("db should open");

        let stats =
            reconcile_library_root(&mut connection, &library_root).expect("scan should succeed");

        let file_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))
            .expect("file count should be readable");
        let directory_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM directories", [], |row| row.get(0))
            .expect("directory count should be readable");

        assert_eq!(stats.files_created, 1);
        assert_eq!(file_count, 1);
        assert_eq!(directory_count, 2);
    }

    #[test]
    fn reconcile_preserves_file_id_across_rename_when_inode_matches() {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let original_path = library_root.join("Kontakt").join("piano.nki");
        let renamed_path = library_root.join("Kontakt").join("piano-renamed.nki");
        fs::write(&original_path, "fixture").expect("fixture file should be written");

        let db_dir = tempdir().expect("db tempdir should be created");
        let mut connection =
            open_metadata_database(&db_dir.path().join("server.sqlite")).expect("db should open");

        reconcile_library_root(&mut connection, &library_root)
            .expect("initial scan should succeed");
        let original_id = file_id_for_path(&connection, &original_path)
            .expect("file should exist after initial scan");

        fs::rename(&original_path, &renamed_path).expect("file should be renamed");
        reconcile_library_root(&mut connection, &library_root).expect("second scan should succeed");
        let renamed_id = file_id_for_path(&connection, &renamed_path)
            .expect("renamed file should exist after second scan");
        let old_path_exists = file_id_for_path(&connection, &original_path);

        assert_eq!(original_id, renamed_id);
        assert!(old_path_exists.is_none());
    }

    #[test]
    fn reconcile_persists_layout_metadata_and_ignores_policy_file_content() {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Samples")).expect("library tree should be created");
        let sample_path = library_root.join("Samples").join("legato.wav");
        fs::write(&sample_path, vec![0_u8; 8 * 1024 * 1024]).expect("sample file should exist");
        fs::write(
            library_root.join(".legato-layout.toml"),
            r#"
[[rule]]
pattern = "*legato.wav"
transfer_class = "random"
extent_bytes = 2097152
"#,
        )
        .expect("policy file should be written");

        let db_dir = tempdir().expect("db tempdir should be created");
        let mut connection =
            open_metadata_database(&db_dir.path().join("server.sqlite")).expect("db should open");

        reconcile_library_root(&mut connection, &library_root).expect("scan should succeed");

        let (file_count, transfer_class, extent_bytes): (i64, i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM files),
                    transfer_class,
                    extent_bytes
                 FROM files
                 WHERE path = ?1",
                [sample_path.to_string_lossy().into_owned()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("catalog entry should be readable");

        assert_eq!(
            file_count, 1,
            "policy file should not be cataloged as content"
        );
        assert_eq!(transfer_class, TransferClass::Random as i64);
        assert_eq!(extent_bytes, 2 * 1024 * 1024);
    }

    fn file_id_for_path(connection: &Connection, path: &Path) -> Option<i64> {
        connection
            .query_row(
                "SELECT file_id FROM files WHERE path = ?1",
                [path.to_string_lossy().into_owned()],
                |row| row.get(0),
            )
            .optional()
            .expect("file lookup should succeed")
    }
}

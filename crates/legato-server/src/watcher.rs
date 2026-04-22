//! Filesystem notification helpers with overflow-safe fallback rescans.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use legato_proto::{InvalidationEvent, InvalidationKind};
use notify::{Config, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Result, Watcher};
use rusqlite::{Connection, OptionalExtension};

use crate::{ReconcileStats, reconcile_library_root, reconcile_paths};

/// The watcher backend selected for a given deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchBackend {
    /// OS-native watcher selected through `notify::recommended_watcher`.
    Recommended,
    /// Poll-based fallback watcher with the provided interval.
    Poll {
        /// Interval between poll scans when native notifications are unavailable.
        interval: Duration,
    },
}

/// Action derived from an incoming filesystem notification result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NotificationAction {
    /// Reconcile the entire library root because targeted handling is unsafe.
    FullRescan,
    /// Reconcile a targeted set of paths derived from the event payload.
    Paths(Vec<PathBuf>),
}

/// Creates and starts the recommended recursive watcher for the library root.
pub fn create_recommended_watcher<F>(library_root: &Path, on_event: F) -> Result<RecommendedWatcher>
where
    F: FnMut(Result<Event>) + Send + 'static,
{
    let mut watcher = notify::recommended_watcher(on_event)?;
    watcher.watch(library_root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

/// Creates and starts the poll-based recursive watcher for the library root.
pub fn create_poll_watcher<F>(
    library_root: &Path,
    interval: Duration,
    on_event: F,
) -> Result<PollWatcher>
where
    F: FnMut(Result<Event>) + Send + 'static,
{
    let mut watcher = PollWatcher::new(on_event, Config::default().with_poll_interval(interval))?;
    watcher.watch(library_root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

/// Converts a notify event or error into a safe reconciliation action.
#[must_use]
pub fn plan_notification_result(library_root: &Path, result: Result<Event>) -> NotificationAction {
    match result {
        Ok(event) => {
            let mut paths = event
                .paths
                .into_iter()
                .filter_map(|path| normalize_event_path(library_root, &path))
                .collect::<Vec<_>>();

            if paths.is_empty() {
                NotificationAction::FullRescan
            } else {
                paths.sort();
                paths.dedup();
                NotificationAction::Paths(paths)
            }
        }
        Err(_error) => NotificationAction::FullRescan,
    }
}

/// Applies a notification result directly to the server metadata DB.
pub fn apply_notification_result(
    connection: &mut Connection,
    library_root: &Path,
    result: Result<Event>,
) -> rusqlite::Result<ReconcileStats> {
    match plan_notification_result(library_root, result) {
        NotificationAction::FullRescan => reconcile_library_root(connection, library_root),
        NotificationAction::Paths(paths) => reconcile_paths(connection, library_root, &paths),
    }
}

/// Converts a planned notification action into client-facing invalidations.
pub fn invalidation_events_for_action(
    connection: &Connection,
    library_root: &Path,
    action: &NotificationAction,
) -> rusqlite::Result<Vec<InvalidationEvent>> {
    match action {
        NotificationAction::FullRescan => Ok(vec![build_invalidation(
            connection,
            library_root,
            InvalidationKind::Subtree,
        )?]),
        NotificationAction::Paths(paths) => paths
            .iter()
            .map(|path| build_invalidation(connection, path, InvalidationKind::Subtree))
            .collect(),
    }
}

fn normalize_event_path(library_root: &Path, path: &Path) -> Option<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        library_root.join(path)
    };

    if candidate == library_root || candidate.starts_with(library_root) {
        if candidate.is_file() {
            candidate
                .parent()
                .map(Path::to_path_buf)
                .or(Some(candidate))
        } else {
            Some(candidate)
        }
    } else {
        None
    }
}

fn build_invalidation(
    connection: &Connection,
    path: &Path,
    fallback_kind: InvalidationKind,
) -> rusqlite::Result<InvalidationEvent> {
    let normalized_path = path.to_string_lossy().into_owned();
    let metadata = lookup_invalidation_metadata(connection, &normalized_path)?;

    Ok(InvalidationEvent {
        kind: metadata
            .as_ref()
            .map_or(fallback_kind as i32, |metadata| metadata.kind as i32),
        path: normalized_path,
        file_id: metadata.map_or(0, |metadata| metadata.file_id),
    })
}

fn lookup_invalidation_metadata(
    connection: &Connection,
    path: &str,
) -> rusqlite::Result<Option<InvalidationMetadata>> {
    if let Some(directory_id) = connection
        .query_row(
            "SELECT directory_id FROM directories WHERE path = ?1",
            [path],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    {
        return Ok(Some(InvalidationMetadata {
            kind: InvalidationKind::Directory,
            file_id: directory_id as u64,
        }));
    }

    connection
        .query_row("SELECT file_id FROM files WHERE path = ?1", [path], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .map(|metadata| {
            metadata.map(|file_id| InvalidationMetadata {
                kind: InvalidationKind::File,
                file_id: file_id as u64,
            })
        })
}

struct InvalidationMetadata {
    kind: InvalidationKind,
    file_id: u64,
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, time::Duration};

    use legato_proto::InvalidationKind;
    use notify::{Event, EventKind};
    use tempfile::tempdir;

    use super::{
        NotificationAction, WatchBackend, apply_notification_result,
        invalidation_events_for_action, plan_notification_result,
    };
    use crate::{open_metadata_database, reconcile_library_root};

    #[test]
    fn notification_errors_force_full_rescan() {
        let root = Path::new("/tmp/legato");
        let action = plan_notification_result(root, Err(notify::Error::generic("overflow")));
        assert_eq!(action, NotificationAction::FullRescan);
    }

    #[test]
    fn notification_paths_are_coalesced_into_targeted_scope() {
        let root = tempdir().expect("tempdir should be created");
        let library_root = root.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let file_path = library_root.join("Kontakt").join("piano.nki");
        fs::write(&file_path, "fixture").expect("fixture file should be written");

        let action = plan_notification_result(
            &library_root,
            Ok(Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Any,
                )),
                paths: vec![file_path.clone(), file_path],
                attrs: Default::default(),
            }),
        );

        assert_eq!(
            action,
            NotificationAction::Paths(vec![library_root.join("Kontakt")])
        );
    }

    #[test]
    fn targeted_notification_rescan_updates_metadata_database() {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let file_path = library_root.join("Kontakt").join("piano.nki");

        let db_dir = tempdir().expect("db tempdir should be created");
        let mut connection =
            open_metadata_database(&db_dir.path().join("server.sqlite")).expect("db should open");

        fs::write(&file_path, "fixture").expect("fixture file should be written");
        let stats = apply_notification_result(
            &mut connection,
            &library_root,
            Ok(Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![file_path.clone()],
                attrs: Default::default(),
            }),
        )
        .expect("notification application should succeed");

        let file_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))
            .expect("file count should be readable");

        assert_eq!(stats.files_created, 1);
        assert_eq!(file_count, 1);
        assert_eq!(
            WatchBackend::Poll {
                interval: Duration::from_secs(5)
            },
            WatchBackend::Poll {
                interval: Duration::from_secs(5)
            }
        );
    }

    #[test]
    fn invalidation_actions_expand_to_subtree_and_file_events() {
        let fixture = tempdir().expect("fixture tempdir should be created");
        let library_root = fixture.path().join("libraries");
        fs::create_dir_all(library_root.join("Kontakt")).expect("library tree should be created");
        let file_path = library_root.join("Kontakt").join("piano.nki");
        fs::write(&file_path, "fixture").expect("fixture file should be written");

        let mut connection =
            open_metadata_database(&fixture.path().join("server.sqlite")).expect("db should open");
        reconcile_library_root(&mut connection, &library_root).expect("reconcile should succeed");

        let full_rescan_events = invalidation_events_for_action(
            &connection,
            &library_root,
            &NotificationAction::FullRescan,
        )
        .expect("full rescan invalidation should succeed");
        assert_eq!(full_rescan_events.len(), 1);
        assert_eq!(
            full_rescan_events[0].kind,
            InvalidationKind::Directory as i32
        );
        assert_eq!(
            full_rescan_events[0].path,
            library_root.to_string_lossy().as_ref()
        );

        let file_events = invalidation_events_for_action(
            &connection,
            &library_root,
            &NotificationAction::Paths(vec![file_path.clone()]),
        )
        .expect("file invalidation should succeed");
        assert_eq!(file_events.len(), 1);
        assert_eq!(file_events[0].kind, InvalidationKind::File as i32);
        assert_eq!(file_events[0].path, file_path.to_string_lossy().as_ref());

        let missing_events = invalidation_events_for_action(
            &connection,
            &library_root,
            &NotificationAction::Paths(vec![library_root.join("Missing")]),
        )
        .expect("missing path invalidation should succeed");
        assert_eq!(missing_events.len(), 1);
        assert_eq!(missing_events[0].kind, InvalidationKind::Subtree as i32);
    }
}

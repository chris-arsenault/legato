//! Filesystem notification helpers with overflow-safe fallback rescans.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use notify::{Config, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Result, Watcher};
use rusqlite::Connection;

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

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, time::Duration};

    use notify::{Event, EventKind};
    use tempfile::tempdir;

    use super::{
        NotificationAction, WatchBackend, apply_notification_result, plan_notification_result,
    };
    use crate::open_metadata_database;

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
}

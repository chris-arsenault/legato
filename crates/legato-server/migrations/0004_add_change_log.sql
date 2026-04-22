CREATE TABLE change_log (
  sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
  kind              INTEGER NOT NULL,
  file_id           INTEGER NOT NULL,
  path              TEXT    NOT NULL,
  is_dir            INTEGER NOT NULL DEFAULT 0,
  size              INTEGER NOT NULL DEFAULT 0,
  mtime_ns          INTEGER NOT NULL DEFAULT 0,
  transfer_class    INTEGER,
  extent_bytes      INTEGER,
  recorded_at_ns    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX change_log_path_sequence_idx
  ON change_log(path, sequence);

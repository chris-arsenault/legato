CREATE TABLE directories (
  directory_id       INTEGER PRIMARY KEY,
  path               TEXT    NOT NULL UNIQUE,
  parent_directory_id INTEGER REFERENCES directories(directory_id) ON DELETE CASCADE,
  mtime_ns           INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE files (
  file_id            INTEGER PRIMARY KEY,
  directory_id       INTEGER NOT NULL REFERENCES directories(directory_id) ON DELETE CASCADE,
  path               TEXT    NOT NULL UNIQUE,
  size               INTEGER NOT NULL,
  mtime_ns           INTEGER NOT NULL,
  content_hash       BLOB,
  created_at_ns      INTEGER NOT NULL DEFAULT 0,
  updated_at_ns      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE watches (
  watch_id           INTEGER PRIMARY KEY,
  path               TEXT    NOT NULL UNIQUE,
  last_event_ns      INTEGER NOT NULL DEFAULT 0,
  overflowed         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE server_state (
  key                TEXT PRIMARY KEY,
  value              TEXT NOT NULL
);

CREATE INDEX directories_parent_idx
  ON directories(parent_directory_id);

CREATE INDEX files_directory_idx
  ON files(directory_id);

CREATE INDEX files_mtime_idx
  ON files(mtime_ns);

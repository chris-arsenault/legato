CREATE TABLE cache_entries (
  file_id           INTEGER NOT NULL,
  start_offset      INTEGER NOT NULL,
  block_count       INTEGER NOT NULL,
  content_hash      BLOB    NOT NULL,
  content_size      INTEGER NOT NULL,
  storage_relative_path TEXT NOT NULL,
  last_access_ns    INTEGER NOT NULL DEFAULT 0,
  pin_generation    INTEGER NOT NULL DEFAULT 0,
  state             TEXT    NOT NULL DEFAULT 'ready',
  PRIMARY KEY (file_id, start_offset)
);

CREATE TABLE pins (
  generation        INTEGER PRIMARY KEY,
  reason            TEXT    NOT NULL,
  created_at_ns     INTEGER NOT NULL
);

CREATE TABLE fetch_state (
  file_id           INTEGER NOT NULL,
  start_offset      INTEGER NOT NULL,
  block_count       INTEGER NOT NULL,
  priority          INTEGER NOT NULL,
  state             TEXT    NOT NULL,
  updated_at_ns     INTEGER NOT NULL,
  PRIMARY KEY (file_id, start_offset)
);

CREATE TABLE client_state (
  key               TEXT PRIMARY KEY,
  value             TEXT NOT NULL
);

CREATE INDEX cache_entries_last_access_idx
  ON cache_entries(last_access_ns);

CREATE INDEX cache_entries_pin_generation_idx
  ON cache_entries(pin_generation);

CREATE INDEX fetch_state_state_idx
  ON fetch_state(state);

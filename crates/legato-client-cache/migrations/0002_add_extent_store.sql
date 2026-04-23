CREATE TABLE IF NOT EXISTS extent_entries (
  file_id              INTEGER NOT NULL,
  extent_index         INTEGER NOT NULL,
  file_offset          INTEGER NOT NULL,
  extent_length        INTEGER NOT NULL,
  transfer_class       INTEGER NOT NULL,
  content_hash         BLOB    NOT NULL,
  content_size         INTEGER NOT NULL,
  storage_relative_path TEXT   NOT NULL,
  last_access_ns       INTEGER NOT NULL DEFAULT 0,
  pin_generation       INTEGER NOT NULL DEFAULT 0,
  state                TEXT    NOT NULL DEFAULT 'ready',
  PRIMARY KEY (file_id, extent_index)
);

CREATE TABLE IF NOT EXISTS extent_fetch_state (
  file_id              INTEGER NOT NULL,
  extent_index         INTEGER NOT NULL,
  priority             INTEGER NOT NULL,
  state                TEXT    NOT NULL,
  updated_at_ns        INTEGER NOT NULL,
  PRIMARY KEY (file_id, extent_index)
);

CREATE INDEX IF NOT EXISTS extent_entries_last_access_idx
  ON extent_entries(last_access_ns);

CREATE INDEX IF NOT EXISTS extent_entries_pin_generation_idx
  ON extent_entries(pin_generation);

CREATE INDEX IF NOT EXISTS extent_fetch_state_state_idx
  ON extent_fetch_state(state);

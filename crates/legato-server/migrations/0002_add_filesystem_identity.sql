ALTER TABLE directories ADD COLUMN device_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE directories ADD COLUMN inode INTEGER NOT NULL DEFAULT 0;

ALTER TABLE files ADD COLUMN device_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE files ADD COLUMN inode INTEGER NOT NULL DEFAULT 0;

CREATE INDEX directories_identity_idx
  ON directories(device_id, inode);

CREATE INDEX files_identity_idx
  ON files(device_id, inode);

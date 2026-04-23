DROP INDEX IF EXISTS cache_entries_last_access_idx;
DROP INDEX IF EXISTS cache_entries_pin_generation_idx;
DROP INDEX IF EXISTS fetch_state_state_idx;

DROP TABLE IF EXISTS cache_entries;
DROP TABLE IF EXISTS fetch_state;

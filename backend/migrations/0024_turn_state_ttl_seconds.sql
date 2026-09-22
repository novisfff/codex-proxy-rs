ALTER TABLE turn_state_fetcher_configs
    ADD COLUMN state_ttl_seconds INTEGER NOT NULL DEFAULT 3600;
UPDATE turn_state_fetcher_configs SET state_ttl_seconds = state_ttl_minutes * 60;
ALTER TABLE turn_state_fetcher_configs
    DROP COLUMN state_ttl_minutes,
    ADD CHECK (state_ttl_seconds BETWEEN 1 AND 86400),
    ADD CHECK (refresh_interval_minutes >= 1 AND refresh_interval_minutes * 60 <= state_ttl_seconds);

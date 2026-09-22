ALTER TABLE turn_state_fetcher_configs
    ADD COLUMN refresh_interval_minutes INTEGER NOT NULL DEFAULT 40,
    ADD COLUMN state_ttl_minutes INTEGER NOT NULL DEFAULT 60,
    ADD CHECK (state_ttl_minutes BETWEEN 1 AND 1440),
    ADD CHECK (refresh_interval_minutes BETWEEN 1 AND state_ttl_minutes);

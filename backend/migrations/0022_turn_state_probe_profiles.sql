ALTER TABLE turn_state_fetcher_configs
    ADD COLUMN probe_profile TEXT NOT NULL DEFAULT 'minimal_compat'
        CHECK (probe_profile IN ('codex_core', 'minimal_compat')),
    ADD COLUMN adaptive_concurrency BOOLEAN NOT NULL DEFAULT true;

CREATE TABLE turn_state_probe_history (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES turn_state_fetcher_configs(account_id) ON DELETE CASCADE,
    started_at BIGINT NOT NULL,
    data JSONB NOT NULL
);
CREATE INDEX turn_state_probe_history_recent ON turn_state_probe_history(started_at DESC, id DESC);

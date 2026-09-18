CREATE TABLE turn_state_fetcher_configs (
    account_id TEXT PRIMARY KEY REFERENCES provider_accounts(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT false,
    models JSONB NOT NULL DEFAULT '[]',
    proxy_id TEXT REFERENCES outbound_proxies(id) ON DELETE RESTRICT,
    revision BIGINT NOT NULL DEFAULT 1,
    CHECK (jsonb_typeof(models) = 'array' AND jsonb_array_length(models) <= 32)
);

CREATE TABLE turn_state_values (
    account_id TEXT NOT NULL REFERENCES provider_accounts(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (account_id, model),
    CHECK (octet_length(data->>'value') = 292)
);

CREATE TABLE turn_state_fetch_attempts (
    account_id TEXT NOT NULL REFERENCES turn_state_fetcher_configs(account_id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (account_id, model)
);

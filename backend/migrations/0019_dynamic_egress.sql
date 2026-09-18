ALTER TABLE turn_state_fetcher_configs
    ADD COLUMN dynamic_egress JSONB;
ALTER TABLE turn_state_fetcher_configs
    ADD CONSTRAINT turn_state_fetcher_single_egress
    CHECK (dynamic_egress IS NULL OR proxy_id IS NULL);

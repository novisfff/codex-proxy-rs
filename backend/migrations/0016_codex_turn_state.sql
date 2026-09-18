ALTER TABLE runtime_settings
    ADD COLUMN codex_turn_state_json JSONB NOT NULL DEFAULT '{"mode":"default","value":""}'::jsonb;

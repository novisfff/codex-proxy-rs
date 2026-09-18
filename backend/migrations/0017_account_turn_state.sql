-- 升级时保留现有账号的请求行为；新账号由 Provider 使用默认模式。
UPDATE provider_accounts
SET provider_credentials_json = jsonb_set(
        provider_credentials_json,
        '{codex_turn_state}',
        (SELECT codex_turn_state_json FROM runtime_settings LIMIT 1)
    ),
    credential_revision = credential_revision + 1
WHERE provider_kind = 'openai'
  AND NOT (provider_credentials_json ? 'codex_turn_state')
  AND EXISTS (SELECT 1 FROM runtime_settings);

-- 全局字段保留用于旧客户端读取，但不再参与请求执行。
UPDATE runtime_settings
SET codex_turn_state_json = '{"mode":"default","value":""}'::jsonb;

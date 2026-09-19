-- NULL 保持已有账号全天探测，按北京时间保存每日起止分钟。
ALTER TABLE turn_state_fetcher_configs ADD COLUMN schedule JSONB;

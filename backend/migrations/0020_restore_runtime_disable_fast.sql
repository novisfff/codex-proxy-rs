-- 保留旧版设置页的全局 Fast 策略字段；分组级策略仍由 account_groups.disable_fast 独立控制。
ALTER TABLE runtime_settings
    ADD COLUMN disable_fast boolean NOT NULL DEFAULT false;

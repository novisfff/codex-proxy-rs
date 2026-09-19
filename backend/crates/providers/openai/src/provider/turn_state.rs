//! 按账号、上游模型共享获取结果；相同值不续期，过期值只供管理端查看。

use crate::transport::protocol::responses::CodexResponsesRequest;
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use gateway_admin::model::settings::AutomaticTurnState;
use gateway_core::{
    policy::{CodexTurnStateConfig, CodexTurnStateMode},
    provider_ports::turn_state::{TurnStateStore, TurnStateValue},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

pub(crate) const STATE_TTL_MS: i64 = 60 * 60 * 1000;
pub(crate) const REFRESH_AFTER_MS: i64 = 40 * 60 * 1000;
pub(crate) const REFRESH_BEFORE_MS: i64 = STATE_TTL_MS - REFRESH_AFTER_MS;

fn expiration(value: &str, acquired_at: i64) -> i64 {
    // Fernet 的版本和大端秒级时间戳是明文；这里只用于调度，不验证签名。
    let generated_at = URL_SAFE.decode(value).ok().and_then(|raw| {
        if raw.first() != Some(&0x80) || raw.len() < 73 || (raw.len() - 57) % 16 != 0 {
            return None;
        }
        let seconds = u64::from_be_bytes(raw.get(1..9)?.try_into().ok()?);
        let millis = i64::try_from(seconds).ok()?.checked_mul(1000)?;
        DateTime::from_timestamp_millis(millis)?;
        Some(millis)
    });
    // 未知格式保持兼容；未来时间不能将有效期延长到获取时间一小时以后。
    generated_at
        .unwrap_or(acquired_at)
        .min(acquired_at)
        .saturating_add(STATE_TTL_MS)
}

#[derive(Clone, Default)]
pub(crate) struct TurnStateCache {
    values: Arc<Mutex<BTreeMap<(String, String), TurnStateValue>>>,
    store: Arc<Mutex<Option<Arc<dyn TurnStateStore>>>>,
    client_retries: Arc<Mutex<BTreeMap<(String, String), ClientRetryState>>>,
}

struct ClientRetryState {
    config_revision: i64,
    failures: usize,
}

impl TurnStateCache {
    pub(crate) fn valid_accounts(&self, model: &str) -> BTreeSet<String> {
        let now = Utc::now().timestamp_millis();
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|v| v.model == model && v.expires_at > now)
            .map(|v| v.account_id.clone())
            .collect()
    }
    pub(crate) fn attach_store(&self, store: Arc<dyn TurnStateStore>) {
        *self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(store);
    }

    pub(super) fn scope(&self, account_id: &str, model: &str) -> ScopedTurnState {
        ScopedTurnState {
            cache: self.clone(),
            key: (account_id.to_owned(), model.to_owned()),
        }
    }

    pub(crate) fn restore(&self, values: Vec<TurnStateValue>) {
        let mut cache = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for mut value in values {
            if valid_value(&value.value) && value.acquired_at <= value.last_seen_at {
                value.expires_at = expiration(&value.value, value.acquired_at);
                cache.insert((value.account_id.clone(), value.model.clone()), value);
            }
        }
    }

    pub(crate) fn values(&self) -> Vec<TurnStateValue> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Vec<AutomaticTurnState> {
        let now = Utc::now().timestamp_millis();
        self.values()
            .into_iter()
            .filter(|v| v.expires_at > now)
            .filter_map(|v| {
                Some(AutomaticTurnState {
                    account_id: v.account_id,
                    model: v.model,
                    value: v.value,
                    acquired_at: DateTime::from_timestamp_millis(v.acquired_at)?,
                })
            })
            .collect()
    }

    pub(crate) fn observe_at(
        &self,
        account_id: &str,
        model: &str,
        value: &str,
        source: &str,
        now: i64,
    ) {
        if !valid_value(value) {
            return;
        }
        let mut cache = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (account_id.to_owned(), model.to_owned());
        if let Some(previous) = cache.get_mut(&key) {
            if previous.last_seen_at > now {
                return;
            }
            if previous.value == value {
                previous.last_seen_at = now;
                return;
            }
        }
        cache.insert(
            key,
            TurnStateValue {
                account_id: account_id.to_owned(),
                model: model.to_owned(),
                value: value.to_owned(),
                acquired_at: now,
                last_seen_at: now,
                expires_at: expiration(value, now),
                source: source.to_owned(),
            },
        );
        if expiration(value, now) > now {
            self.client_retries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&(account_id.to_owned(), model.to_owned()));
        }
    }
}

pub(crate) fn valid_value(value: &str) -> bool {
    value.len() == 292 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

pub(super) struct ScopedTurnState {
    cache: TurnStateCache,
    key: (String, String),
}
impl ScopedTurnState {
    pub(super) async fn missing_state_retry(
        &self,
        automatic: bool,
        returned_length: usize,
    ) -> Option<u64> {
        if !automatic || returned_length != 312 {
            return None;
        }
        let store = self
            .cache
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let configs = store.configs().await.ok()?;
        let config = configs
            .iter()
            .find(|c| c.enabled && c.account_id == self.key.0 && c.models.contains(&self.key.1))?;
        // 与观察新值采用相同锁顺序，避免并发获取成功后又增加旧的退避次数。
        let values = self
            .cache
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if values
            .get(&self.key)
            .is_some_and(|v| v.expires_at > Utc::now().timestamp_millis())
        {
            return None;
        }
        let mut retries = self
            .cache
            .client_retries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = retries.entry(self.key.clone()).or_insert(ClientRetryState {
            config_revision: config.revision,
            failures: 0,
        });
        if entry.config_revision != config.revision {
            entry.config_revision = config.revision;
            entry.failures = 0;
        }
        let seconds = [30, 60, 180, 300, 600][entry.failures.min(4)];
        entry.failures = entry.failures.saturating_add(1);
        Some(seconds)
    }

    pub(super) fn observe(&self, value: &str) {
        self.cache.observe_at(
            &self.key.0,
            &self.key.1,
            value,
            "traffic",
            Utc::now().timestamp_millis(),
        );
    }
    pub(super) fn observe_headers(&self, headers: &[(String, Bytes)]) {
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("x-codex-turn-state")
                && let Ok(value) = std::str::from_utf8(value)
            {
                self.observe(value);
            }
        }
    }
    pub(super) fn apply(&self, request: &mut CodexResponsesRequest, config: &CodexTurnStateConfig) {
        let value = match config.mode {
            CodexTurnStateMode::Default => None,
            CodexTurnStateMode::Manual => Some(config.value.clone()),
            CodexTurnStateMode::Auto => self
                .cache
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&self.key)
                .filter(|v| v.expires_at > Utc::now().timestamp_millis())
                .map(|v| v.value.clone()),
        };
        if config.mode == CodexTurnStateMode::Auto || value.is_some() {
            request.passthrough_headers.remove("x-codex-turn-state");
            request.turn_state = value;
        }
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    fn token(seconds: u64) -> String {
        let mut raw = vec![0u8; 217];
        raw[0] = 0x80;
        raw[1..9].copy_from_slice(&seconds.to_be_bytes());
        URL_SAFE.encode(raw)
    }

    #[test]
    fn turn_state_generation_time_controls_expiry_and_refresh() {
        let generated = 1_800_000_000_000;
        let acquired = generated + 45 * 60 * 1000;
        let cache = TurnStateCache::default();
        let value = token((generated / 1000) as u64);
        assert_eq!(value.len(), 292);
        cache.observe_at("account", "model", &value, "fetcher", acquired);
        let entry = cache.values().remove(0);
        assert_eq!(entry.acquired_at, acquired);
        assert_eq!(entry.expires_at, generated + STATE_TTL_MS);
        assert!(entry.expires_at - REFRESH_BEFORE_MS < acquired);
        cache.observe_at("account", "model", &value, "traffic", acquired + 1000);
        assert_eq!(cache.values()[0].expires_at, entry.expires_at);
        let mut old = entry;
        old.expires_at = acquired + STATE_TTL_MS;
        let restored = TurnStateCache::default();
        restored.restore(vec![old]);
        assert_eq!(restored.values()[0].expires_at, generated + STATE_TTL_MS);
    }

    #[test]
    fn turn_state_routing_excludes_expired_values_and_other_models() {
        let now = Utc::now().timestamp_millis();
        let cache = TurnStateCache::default();
        cache.observe_at(
            "ready",
            "model-a",
            &token((now / 1000) as u64),
            "traffic",
            now,
        );
        cache.observe_at(
            "expired",
            "model-a",
            &token((now / 1000 - 3601) as u64),
            "traffic",
            now,
        );
        cache.observe_at(
            "other",
            "model-b",
            &token((now / 1000) as u64),
            "traffic",
            now,
        );
        assert_eq!(
            cache.valid_accounts("model-a"),
            BTreeSet::from(["ready".to_owned()])
        );
        assert!(cache.valid_accounts("missing").is_empty());
    }

    #[test]
    fn turn_state_expired_future_and_invalid_timestamps_are_bounded() {
        let now = 1_800_000_000_000;
        assert!(expiration(&token((now / 1000 - 3601) as u64), now) < now);
        assert_eq!(
            expiration(&token((now / 1000 + 600) as u64), now),
            now + STATE_TTL_MS
        );
        for value in ["a".repeat(292), token(u64::MAX), "%%%".to_owned()] {
            assert_eq!(expiration(&value, now), now + STATE_TTL_MS);
        }
        let mut wrong_version = URL_SAFE.decode(token(1_800_000_000)).unwrap();
        wrong_version[0] = 0x81;
        assert_eq!(
            expiration(&URL_SAFE.encode(wrong_version), now),
            now + STATE_TTL_MS
        );
    }
}

//! 按账号、上游模型共享获取结果；相同值不续期，过期值只供管理端查看。

use crate::transport::protocol::responses::CodexResponsesRequest;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use gateway_admin::model::settings::AutomaticTurnState;
use gateway_core::{
    policy::{CodexTurnStateConfig, CodexTurnStateMode},
    provider_ports::turn_state::TurnStateValue,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) const STATE_TTL_MS: i64 = 60 * 60 * 1000;
pub(crate) const REFRESH_AFTER_MS: i64 = 40 * 60 * 1000;

#[derive(Clone, Default)]
pub(crate) struct TurnStateCache {
    values: Arc<Mutex<BTreeMap<(String, String), TurnStateValue>>>,
}

impl TurnStateCache {
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
        for value in values {
            if valid_value(&value.value)
                && value.acquired_at <= value.last_seen_at
                && value.expires_at == value.acquired_at.saturating_add(STATE_TTL_MS)
            {
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
                expires_at: now.saturating_add(STATE_TTL_MS),
                source: source.to_owned(),
            },
        );
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

//! 按实际账号和上游模型隔离 Turn State；思考强度不参与缓存键。

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use chrono::Utc;
use gateway_admin::model::settings::AutomaticTurnState;
use gateway_core::policy::{CodexTurnStateConfig, CodexTurnStateMode};

use crate::transport::protocol::responses::CodexResponsesRequest;

#[derive(Clone, Default)]
pub(crate) struct TurnStateCache {
    values: Arc<Mutex<BTreeMap<(String, String), AutomaticTurnState>>>,
}

impl TurnStateCache {
    pub(super) fn scope(&self, account_id: &str, model: &str) -> ScopedTurnState {
        ScopedTurnState {
            cache: self.clone(),
            key: (account_id.to_owned(), model.to_owned()),
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<AutomaticTurnState> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

pub(super) struct ScopedTurnState {
    cache: TurnStateCache,
    key: (String, String),
}

impl ScopedTurnState {
    pub(super) fn observe(&self, value: &str) {
        if value.len() == 292 && value.bytes().all(|byte| byte.is_ascii_graphic()) {
            let mut current = self
                .cache
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            current.insert(
                self.key.clone(),
                AutomaticTurnState {
                    account_id: self.key.0.clone(),
                    model: self.key.1.clone(),
                    value: value.to_owned(),
                    acquired_at: Utc::now(),
                },
            );
        }
    }

    fn snapshot(&self) -> Option<AutomaticTurnState> {
        self.cache
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&self.key)
            .cloned()
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
            CodexTurnStateMode::Auto => self.snapshot().map(|state| state.value),
        };
        if config.mode == CodexTurnStateMode::Auto || value.is_some() {
            // 账号隔离已完成；管理员覆盖必须同时压过原始透传头，避免 HTTP 多值或 WS 两处不一致。
            request.passthrough_headers.remove("x-codex-turn-state");
            request.turn_state = value;
        }
    }
}

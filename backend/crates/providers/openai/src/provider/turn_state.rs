//! 显式启用的全站 Turn State 覆盖；只保存最近一次观测到的有效上游值。

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use gateway_core::policy::{CodexTurnStateConfig, CodexTurnStateMode};

use crate::transport::protocol::responses::CodexResponsesRequest;

#[derive(Clone, Default)]
pub(super) struct GlobalTurnState {
    value: Arc<Mutex<Option<String>>>,
}

impl GlobalTurnState {
    pub(super) fn observe(&self, value: &str) {
        if value.len() == 292 && value.bytes().all(|byte| byte.is_ascii_graphic()) {
            *self
                .value
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(value.to_owned());
        }
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
                .value
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        };
        if let Some(value) = value {
            // 账号隔离已完成；管理员覆盖必须同时压过原始透传头，避免 HTTP 多值或 WS 两处不一致。
            request.passthrough_headers.remove("x-codex-turn-state");
            request.turn_state = Some(value);
        }
    }
}

//! 管理员显式配置的 Codex Turn State 覆盖策略；缓存与协议应用由 OpenAI Provider 拥有。

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::validation::IdentifierError;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexTurnStateMode {
    #[default]
    Default,
    Manual,
    Auto,
}

#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodexTurnStateConfig {
    pub mode: CodexTurnStateMode,
    pub value: String,
}

impl CodexTurnStateConfig {
    /// 只接受可直接发送的 ASCII 头值；不静默裁剪不透明状态。
    pub fn validate(&self) -> Result<(), IdentifierError> {
        if self.value.len() > 8 * 1024
            || !self.value.bytes().all(|byte| byte.is_ascii_graphic())
            || (self.mode == CodexTurnStateMode::Manual && self.value.is_empty())
        {
            return Err(IdentifierError::InvalidFormat);
        }
        Ok(())
    }
}

impl fmt::Debug for CodexTurnStateConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexTurnStateConfig")
            .field("mode", &self.mode)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

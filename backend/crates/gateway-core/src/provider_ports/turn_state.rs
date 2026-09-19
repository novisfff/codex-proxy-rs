//! Turn State 获取器的持久化边界；时间使用 Unix 毫秒，秘密值不进入 Debug。

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use super::ProviderStoreError;
use crate::account::OutboundProxy;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateFetcherConfig {
    pub account_id: String,
    pub enabled: bool,
    pub models: Vec<String>,
    pub proxy_id: Option<String>,
    #[serde(default)]
    pub dynamic_egress: Option<DynamicEgressSelection>,
    pub revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DynamicEgressSelection {
    pub instance: String,
    pub family: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateValue {
    pub account_id: String,
    pub model: String,
    pub value: String,
    pub acquired_at: i64,
    pub last_seen_at: i64,
    pub expires_at: i64,
    pub source: String,
}

impl std::fmt::Debug for TurnStateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnStateValue")
            .field("account_id", &self.account_id)
            .field("model", &self.model)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateFetchAttempt {
    pub account_id: String,
    pub model: String,
    pub config_revision: i64,
    pub attempted_at: i64,
    pub next_attempt_at: i64,
    pub failures: u32,
    pub paused: bool,
    pub status: String,
    pub message: String,
    pub byte_length: Option<usize>,
    pub duration_ms: u64,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub exit_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateFetcherSnapshot {
    pub configs: Vec<TurnStateFetcherConfig>,
    pub values: Vec<TurnStateValue>,
    pub attempts: Vec<TurnStateFetchAttempt>,
    pub running: Option<(String, String)>,
    pub running_requests: Vec<(String, String)>,
    pub dynamic_egress: serde_json::Value,
}

#[derive(Debug)]
pub struct TurnStateFetchEgress {
    pub config_revision: crate::routing::ConfigRevision,
    pub proxy: Option<OutboundProxy>,
    pub max_concurrent: u32,
    pub request_interval_ms: u64,
}

pub trait TurnStateStore: Send + Sync {
    fn configs(&self) -> BoxFuture<'_, Result<Vec<TurnStateFetcherConfig>, ProviderStoreError>>;
    fn save_config(
        &self,
        config: TurnStateFetcherConfig,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;
    fn values(&self) -> BoxFuture<'_, Result<Vec<TurnStateValue>, ProviderStoreError>>;
    fn save_values(
        &self,
        values: Vec<TurnStateValue>,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;
    fn attempts(&self) -> BoxFuture<'_, Result<Vec<TurnStateFetchAttempt>, ProviderStoreError>>;
    fn save_attempt(
        &self,
        attempt: TurnStateFetchAttempt,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;
    fn egress(
        &self,
        config: TurnStateFetcherConfig,
    ) -> BoxFuture<'_, Result<TurnStateFetchEgress, ProviderStoreError>>;
}

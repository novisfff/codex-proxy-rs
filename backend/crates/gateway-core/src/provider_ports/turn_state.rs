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
    #[serde(default)]
    pub schedule: Option<TurnStateFetcherSchedule>,
    #[serde(default)]
    pub probe_profile: TurnStateProbeProfile,
    #[serde(default)]
    pub adaptive_concurrency: bool,
    pub revision: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStateProbeProfile {
    #[default]
    CodexCore,
    MinimalCompat,
}

/// 每日北京时间探测窗口；空配置表示全天，起止分钟使用半开区间并允许跨午夜。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStateFetcherSchedule {
    pub start_minute: u16,
    pub end_minute: u16,
}

impl TurnStateFetcherSchedule {
    pub fn is_valid(&self) -> bool {
        self.start_minute < 1440 && self.end_minute < 1440 && self.start_minute != self.end_minute
    }

    /// 返回当前窗口剩余毫秒；时段外或配置非法时返回零。
    pub fn remaining_ms(&self, now_ms: i64) -> u64 {
        if !self.is_valid() {
            return 0;
        }
        let day_ms = 86_400_000;
        let current = (now_ms.rem_euclid(day_ms) + 8 * 3_600_000) % day_ms;
        let start = i64::from(self.start_minute) * 60_000;
        let end = i64::from(self.end_minute) * 60_000;
        let active = if start < end {
            current >= start && current < end
        } else {
            current >= start || current < end
        };
        if active {
            (end - current).rem_euclid(day_ms).unsigned_abs()
        } else {
            0
        }
    }
}

impl TurnStateFetcherConfig {
    pub fn allows_probe_at(&self, now_ms: i64) -> bool {
        self.schedule
            .as_ref()
            .is_none_or(|schedule| schedule.remaining_ms(now_ms) > 0)
    }
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
    #[serde(default)]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub session: Option<TurnStateSession>,
}

/// 随票据保存的上游会话身份；请求及 turn 身份不跨请求复用。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateSession {
    pub session_id: String,
    pub thread_id: String,
    pub window_id: String,
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
    #[serde(default)]
    pub search_concurrency: Option<usize>,
}

/// 有界探测历史，仅保留诊断元数据，不保存票据、正文或代理认证信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStateProbeRecord {
    pub id: String,
    pub batch_id: String,
    pub account_id: String,
    pub model: String,
    pub config_revision: i64,
    pub profile: TurnStateProbeProfile,
    pub started_at: i64,
    pub duration_ms: u64,
    pub outcome: String,
    pub http_status: Option<u16>,
    pub http_version: Option<String>,
    pub byte_length: Option<usize>,
    pub repeated: bool,
    pub endpoint: Option<String>,
    pub responses_lite: bool,
    pub compressed: bool,
    pub egress_instance: Option<String>,
    pub lease_id: Option<String>,
    pub exit_ip: Option<String>,
    pub fresh_connection: bool,
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
    pub recent_probes: Vec<TurnStateProbeRecord>,
}

#[derive(Debug)]
pub struct TurnStateFetchEgress {
    pub config_revision: crate::routing::ConfigRevision,
    pub proxy: Option<OutboundProxy>,
    pub max_concurrent: u32,
    pub request_interval_ms: u64,
}

pub trait TurnStateStore: Send + Sync {
    fn recent_probes(&self)
    -> BoxFuture<'_, Result<Vec<TurnStateProbeRecord>, ProviderStoreError>>;
    fn save_probe(
        &self,
        probe: TurnStateProbeRecord,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;
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

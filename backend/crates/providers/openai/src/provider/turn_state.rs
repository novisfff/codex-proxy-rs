//! 按账号、上游模型共享获取结果；相同值不续期，过期值只供管理端查看。

use crate::transport::protocol::responses::CodexResponsesRequest;
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use gateway_admin::model::settings::AutomaticTurnState;
use gateway_core::{
    policy::{CodexTurnStateConfig, CodexTurnStateMode},
    provider_ports::turn_state::{TurnStateSession, TurnStateStore, TurnStateValue},
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
            proxy_url: None,
            session: None,
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
        (account_id, model): (&str, &str),
        value: &str,
        source: &str,
        proxy_url: Option<&str>,
        session: Option<&TurnStateSession>,
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
                previous.proxy_url = proxy_url.map(str::to_owned);
                previous.session = session.cloned();
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
                proxy_url: proxy_url.map(str::to_owned),
                session: session.cloned(),
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
    proxy_url: Option<String>,
    session: Option<TurnStateSession>,
}
impl ScopedTurnState {
    pub(super) fn session(&self) -> Option<&TurnStateSession> {
        self.session.as_ref()
    }

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
            (&self.key.0, &self.key.1),
            value,
            "traffic",
            self.proxy_url.as_deref(),
            self.session.as_ref(),
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
    pub(super) fn bind_request(
        &mut self,
        request: &CodexResponsesRequest,
        proxy: Option<&gateway_core::account::OutboundProxy>,
    ) {
        self.proxy_url = proxy.map(|proxy| proxy.expose_url().to_owned());
        self.session = request
            .client_session_id
            .as_ref()
            .zip(request.client_thread_id.as_ref())
            .zip(request.codex_window_id.as_ref())
            .map(|((session_id, thread_id), window_id)| TurnStateSession {
                session_id: session_id.clone(),
                thread_id: thread_id.clone(),
                window_id: window_id.clone(),
            });
    }

    pub(super) fn apply(
        &mut self,
        request: &mut CodexResponsesRequest,
        config: &CodexTurnStateConfig,
    ) -> Result<
        Option<gateway_core::account::OutboundProxy>,
        gateway_core::account::InvalidOutboundProxy,
    > {
        // 同一次快照读取票据及出口，避免后台刷新将两个不同探测的结果拼在一起。
        let cached = self
            .cache
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&self.key)
            .filter(|v| v.expires_at > Utc::now().timestamp_millis())
            .cloned();
        let proxy = if config.mode == CodexTurnStateMode::Auto {
            cached
                .as_ref()
                .and_then(|v| v.proxy_url.as_deref())
                .map(|url| {
                    // 探测将 socks5 按远端 DNS 处理；正式流量必须使用相同语义。
                    let remote_dns = url
                        .strip_prefix("socks5://")
                        .map(|rest| format!("socks5h://{rest}"));
                    gateway_core::account::OutboundProxy::parse(
                        remote_dns.as_deref().unwrap_or(url),
                    )
                })
                .transpose()?
        } else {
            None
        };
        let value = match config.mode {
            CodexTurnStateMode::Default => None,
            CodexTurnStateMode::Manual => Some(config.value.clone()),
            CodexTurnStateMode::Auto => cached.map(|v| {
                if let Some(session) = &v.session {
                    apply_session(request, session);
                }
                self.session = v.session;
                v.value
            }),
        };
        if config.mode == CodexTurnStateMode::Auto || value.is_some() {
            request.passthrough_headers.remove("x-codex-turn-state");
            // 同账号续接可能保留正文中的旧票据，自动选择必须同时覆盖所有 wire 投影。
            for key in ["turnState", "turn_state", "x-codex-turn-state"] {
                request.replace_existing_identity_field(key, value.as_deref());
                if let Some(metadata) = request
                    .body_mut()
                    .get_mut("client_metadata")
                    .and_then(serde_json::Value::as_object_mut)
                    && metadata.contains_key(key)
                {
                    if let Some(value) = &value {
                        metadata.insert(key.to_owned(), serde_json::Value::String(value.clone()));
                    } else {
                        metadata.remove(key);
                    }
                }
            }
            request.turn_state = value;
        }
        Ok(proxy)
    }
}

fn apply_session(request: &mut CodexResponsesRequest, session: &TurnStateSession) {
    use serde_json::{Map, Value, json};

    // 本地会话键已经用于亲和和续接；这里只替换上游身份，保持不同客户端的连接隔离。
    request.client_session_id = Some(session.session_id.clone());
    request.client_thread_id = Some(session.thread_id.clone());
    request.codex_window_id = Some(session.window_id.clone());
    request
        .client_request_id
        .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
    let turn_id = request
        .client_turn_id
        .get_or_insert_with(|| uuid::Uuid::new_v4().to_string())
        .clone();
    let mut turn_metadata = request
        .turn_metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Map<String, Value>>(value).ok())
        .unwrap_or_default();
    turn_metadata.insert("session_id".to_owned(), json!(session.session_id));
    turn_metadata.insert("thread_id".to_owned(), json!(session.thread_id));
    turn_metadata.insert("window_id".to_owned(), json!(session.window_id));
    turn_metadata.insert("turn_id".to_owned(), json!(turn_id));
    turn_metadata
        .entry("root_turn_id")
        .or_insert_with(|| json!(turn_id));
    let Some(turn_metadata) = crate::transport::request::encode_turn_metadata(&turn_metadata)
    else {
        return;
    };
    request.turn_metadata = Some(turn_metadata.clone());
    for name in [
        "session-id",
        "session_id",
        "thread-id",
        "x-codex-window-id",
        "x-codex-turn-metadata",
        "x-client-request-id",
    ] {
        request.passthrough_headers.remove(name);
    }
    let body = request.body_mut();
    body.insert("prompt_cache_key".to_owned(), json!(session.session_id));
    for key in ["turnMetadata", "turn_metadata", "x-codex-turn-metadata"] {
        if body.contains_key(key) {
            body.insert(key.to_owned(), json!(turn_metadata));
        }
    }
    // 同时更新旧客户端可能携带的顶层身份，避免与 metadata 的投影相互矛盾。
    for (name, value) in [
        ("session_id", &session.session_id),
        ("thread_id", &session.thread_id),
        ("x-codex-window-id", &session.window_id),
    ] {
        if body.contains_key(name) {
            body.insert(name.to_owned(), json!(value));
        }
    }
    let metadata = body.entry("client_metadata").or_insert_with(|| json!({}));
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert("session_id".to_owned(), json!(session.session_id));
        metadata.insert("thread_id".to_owned(), json!(session.thread_id));
        metadata.insert("x-codex-window-id".to_owned(), json!(session.window_id));
        metadata.insert("turn_id".to_owned(), json!(turn_id));
        for key in ["turnMetadata", "turn_metadata"] {
            if metadata.contains_key(key) {
                metadata.insert(key.to_owned(), json!(turn_metadata));
            }
        }
        metadata.insert("x-codex-turn-metadata".to_owned(), json!(turn_metadata));
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
        cache.observe_at(
            ("account", "model"),
            &value,
            "fetcher",
            None,
            None,
            acquired,
        );
        let entry = cache.values().remove(0);
        assert_eq!(entry.acquired_at, acquired);
        assert_eq!(entry.expires_at, generated + STATE_TTL_MS);
        assert!(entry.expires_at - REFRESH_BEFORE_MS < acquired);
        cache.observe_at(
            ("account", "model"),
            &value,
            "traffic",
            None,
            None,
            acquired + 1000,
        );
        assert_eq!(cache.values()[0].expires_at, entry.expires_at);
        let mut old = entry;
        old.expires_at = acquired + STATE_TTL_MS;
        let restored = TurnStateCache::default();
        restored.restore(vec![old]);
        assert_eq!(restored.values()[0].expires_at, generated + STATE_TTL_MS);
    }

    #[test]
    fn probe_proxy_updates_for_same_value_without_renewing_expiry() {
        let acquired = 1_800_000_000_000;
        let cache = TurnStateCache::default();
        let value = token((acquired / 1000) as u64);
        let other = token((acquired / 1000 + 1) as u64);

        cache.observe_at(
            ("account", "model"),
            &value,
            "fetcher",
            Some("socks5h://first"),
            None,
            acquired,
        );
        let original_expiry = cache.values()[0].expires_at;
        cache.observe_at(
            ("account", "model"),
            &value,
            "fetcher",
            Some("socks5h://latest"),
            None,
            acquired + 1_000,
        );
        assert_eq!(
            cache.values()[0].proxy_url.as_deref(),
            Some("socks5h://latest")
        );
        assert_eq!(cache.values()[0].expires_at, original_expiry);

        cache.observe_at(
            ("account", "model"),
            &other,
            "traffic",
            None,
            None,
            acquired + 2_000,
        );
        assert_eq!(cache.values()[0].proxy_url, None);
    }

    #[test]
    fn automatic_state_and_proxy_are_selected_together_and_observations_keep_egress() {
        use gateway_core::operation::{GenerateRequest, ProtocolPayload};
        let now = Utc::now().timestamp_millis();
        let cache = TurnStateCache::default();
        let value = token((now / 1000) as u64);
        let proxy = "socks5h://user:password@localhost:1080";
        cache.observe_at(
            ("account", "model"),
            &value,
            "fetcher",
            Some(proxy),
            None,
            now,
        );
        let payload = ProtocolPayload::json_object(
            "openai",
            serde_json::from_value(serde_json::json!({"model":"model", "input":"test"})).unwrap(),
        )
        .unwrap();
        let mut request = crate::transport::encode_generate_request(
            &GenerateRequest::from_protocol_payload(payload),
            "model",
            None,
        )
        .unwrap();
        let mut scope = cache.scope("account", "model");
        let config = CodexTurnStateConfig {
            mode: CodexTurnStateMode::Auto,
            value: String::new(),
        };
        let selected = scope.apply(&mut request, &config).unwrap().unwrap();
        assert_eq!(selected.expose_url(), proxy);
        assert_eq!(request.turn_state.as_deref(), Some(value.as_str()));
        scope.bind_request(&request, Some(&selected));
        let next = "b".repeat(292);
        scope.observe(&next);
        assert_eq!(cache.values()[0].proxy_url.as_deref(), Some(proxy));
        assert_eq!(cache.values()[0].value, next);
        assert!(
            scope
                .apply(
                    &mut request,
                    &CodexTurnStateConfig {
                        mode: CodexTurnStateMode::Manual,
                        value: "manual".to_owned()
                    }
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(request.turn_state.as_deref(), Some("manual"));
        assert!(
            cache
                .scope("account", "other")
                .apply(&mut request, &config)
                .unwrap()
                .is_none()
        );
        assert!(request.turn_state.is_none());
        cache.observe_at(
            ("account", "model"),
            &"c".repeat(292),
            "fetcher",
            Some("invalid proxy"),
            None,
            now + 1000,
        );
        assert!(scope.apply(&mut request, &config).is_err());
        cache.observe_at(
            ("account", "model"),
            &token((now / 1000 - 7200) as u64),
            "fetcher",
            Some(proxy),
            None,
            now + 2000,
        );
        assert!(scope.apply(&mut request, &config).unwrap().is_none());
        assert!(request.turn_state.is_none());
    }

    #[test]
    fn old_turn_state_json_defaults_probe_proxy_to_none() {
        let value = serde_json::json!({
            "accountId": "account",
            "model": "model",
            "value": "a".repeat(292),
            "acquiredAt": 1000,
            "lastSeenAt": 1000,
            "expiresAt": 3601000,
            "source": "traffic"
        });
        let restored: TurnStateValue = serde_json::from_value(value).unwrap();
        assert_eq!(restored.proxy_url, None);
        assert!(restored.session.is_none());
    }

    #[test]
    fn automatic_session_overrides_wire_identity_without_changing_local_conversation() {
        use gateway_core::operation::{GenerateRequest, ProtocolPayload};
        use serde_json::json;
        let now = Utc::now().timestamp_millis();
        let cache = TurnStateCache::default();
        let session = TurnStateSession {
            session_id: "probe-session".to_owned(),
            thread_id: "probe-thread".to_owned(),
            window_id: "probe-window".to_owned(),
        };
        cache.observe_at(
            ("account", "model"),
            &"a".repeat(292),
            "fetcher",
            Some("socks5://localhost:1080"),
            Some(&session),
            now,
        );
        let payload = ProtocolPayload::json_object(
            "openai",
            serde_json::from_value(json!({
                "model": "model", "input": "test", "prompt_cache_key": "client-cache",
                "client_metadata": {"session_id": "client-session", "thread_id": "client-thread", "x-codex-turn-state": "stale-state"}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut request = crate::transport::encode_generate_request(
            &GenerateRequest::from_protocol_payload(payload),
            "model",
            None,
        )
        .unwrap();
        request.local_conversation_id = Some("private-local-key".to_owned());
        request.client_request_id = Some("request-current".to_owned());
        request.client_turn_id = Some("turn-current".to_owned());
        request.turn_metadata =
            Some(json!({"cwd":"/工作区", "root_turn_id":"root-current"}).to_string());
        request
            .passthrough_headers
            .insert("session-id", "stale-session".parse().unwrap());
        let mut scope = cache.scope("account", "model");
        let proxy = scope
            .apply(
                &mut request,
                &CodexTurnStateConfig {
                    mode: CodexTurnStateMode::Auto,
                    value: String::new(),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(proxy.expose_url(), "socks5h://localhost:1080");
        assert_eq!(
            request.local_conversation_id.as_deref(),
            Some("private-local-key")
        );
        assert_eq!(
            request.client_request_id.as_deref(),
            Some("request-current")
        );
        assert_eq!(request.client_turn_id.as_deref(), Some("turn-current"));
        assert!(!request.passthrough_headers.contains_key("session-id"));
        assert!(request.turn_metadata.as_ref().unwrap().is_ascii());
        let metadata: serde_json::Value =
            serde_json::from_str(request.turn_metadata.as_ref().unwrap()).unwrap();
        assert_eq!(metadata["cwd"], "/工作区");
        assert_eq!(metadata["root_turn_id"], "root-current");
        assert_eq!(metadata["session_id"], "probe-session");
        assert_eq!(
            request.body()["client_metadata"]["x-codex-turn-state"],
            "a".repeat(292)
        );
        scope.bind_request(&request, Some(&proxy));
        // 后台刷新后，正在执行的请求仍应记录自己实际使用的会话及出口。
        cache.observe_at(
            ("account", "model"),
            &"b".repeat(292),
            "fetcher",
            None,
            None,
            now,
        );
        scope.observe(&"c".repeat(292));
        assert!(cache.values()[0].session.as_ref() == Some(&session));
        assert_eq!(
            cache.values()[0].proxy_url.as_deref(),
            Some("socks5h://localhost:1080")
        );
        let restored = TurnStateCache::default();
        restored.restore(
            serde_json::from_str(&serde_json::to_string(&cache.values()).unwrap()).unwrap(),
        );
        assert!(restored.values()[0].session.as_ref() == Some(&session));
    }

    #[test]
    fn turn_state_routing_excludes_expired_values_and_other_models() {
        let now = Utc::now().timestamp_millis();
        let cache = TurnStateCache::default();
        cache.observe_at(
            ("ready", "model-a"),
            &token((now / 1000) as u64),
            "traffic",
            None,
            None,
            now,
        );
        cache.observe_at(
            ("expired", "model-a"),
            &token((now / 1000 - 3601) as u64),
            "traffic",
            None,
            None,
            now,
        );
        cache.observe_at(
            ("other", "model-b"),
            &token((now / 1000) as u64),
            "traffic",
            None,
            None,
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

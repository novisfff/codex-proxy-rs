//! 独立低并发获取任务。凭据、账号额度与业务共用；连接池、出站代理与用量记录独立。

use super::turn_state::{REFRESH_AFTER_MS, TurnStateCache};
use crate::{
    credential::{CodexCredentialCatalogService, CodexCredentialCodec},
    transport::{
        CodexBackendClient, CodexClientError, CodexRequestContext,
        client::build_account_http_client,
        diagnostics::{CodexFailureCategory, CodexUpstreamFailure},
        encode_generate_request,
        profile::CodexWireProfileState,
    },
};
use chrono::Utc;
use futures::{StreamExt, future::BoxFuture};
use gateway_core::{
    account::{CredentialState, ProviderAccountId, ProviderAccountStore},
    lifecycle::CancellationToken,
    operation::{GenerateRequest, ProtocolPayload},
    provider_ports::{
        ProviderCooldownPort, ProviderCooldownScope, ProviderLeaseAcquisition, ProviderLeasePort,
        ProviderLeaseRequest, ProviderSchedulingLeaseRequest, ProviderStoreError,
        ProviderStoreErrorKind, turn_state::*,
    },
    routing::{ProviderKind, UpstreamModelId},
    task::{ScheduledTask, WorkerCycleContext, WorkerTaskError},
};
use gateway_protocol::openai::sse::SseEventDecoder;
use secrecy::ExposeSecret;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

struct Running {
    account: String,
    model: String,
    cancellation: CancellationToken,
}

pub(crate) struct TurnStateFetcher {
    store: Arc<dyn TurnStateStore>,
    accounts: Arc<dyn ProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    catalog: Arc<CodexCredentialCatalogService>,
    cache: TurnStateCache,
    profile: CodexWireProfileState,
    base_url: String,
    dynamic_egress: Option<super::dynamic_egress::DynamicEgress>,
    running: Mutex<Option<Running>>,
    cycle: tokio::sync::Mutex<()>,
    persisted: Mutex<Vec<TurnStateValue>>,
    updates: tokio::sync::Mutex<()>,
}

fn invalid() -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, "turn state fetcher")
}

impl TurnStateFetcher {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<dyn TurnStateStore>,
        accounts: Arc<dyn ProviderAccountStore>,
        leases: Arc<dyn ProviderLeasePort>,
        cooldowns: Arc<dyn ProviderCooldownPort>,
        catalog: Arc<CodexCredentialCatalogService>,
        cache: TurnStateCache,
        profile: CodexWireProfileState,
        base_url: String,
    ) -> Self {
        Self {
            store,
            accounts,
            leases,
            cooldowns,
            catalog,
            cache,
            profile,
            base_url,
            dynamic_egress: None,
            running: Mutex::new(None),
            cycle: tokio::sync::Mutex::new(()),
            persisted: Mutex::new(Vec::new()),
            updates: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn with_dynamic_egress(
        mut self,
        config: Option<&crate::config::DynamicEgressConfig>,
    ) -> Result<Self, ()> {
        self.dynamic_egress = config
            .map(super::dynamic_egress::DynamicEgress::new)
            .transpose()?;
        Ok(self)
    }

    pub(crate) async fn snapshot(&self) -> Result<TurnStateFetcherSnapshot, ProviderStoreError> {
        Ok(TurnStateFetcherSnapshot {
            dynamic_egress: match &self.dynamic_egress {
                Some(service) => service.status().await,
                None => {
                    json!({"available":false,"message":"尚未配置动态出口服务","instances":[],"history":[]})
                }
            },
            configs: self.store.configs().await?,
            values: self.cache.values(),
            attempts: self.store.attempts().await?,
            running: self
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|r| (r.account.clone(), r.model.clone())),
        })
    }

    pub(crate) async fn configure_dynamic_egress(
        &self,
        config: Value,
    ) -> Result<(), ProviderStoreError> {
        let _update = self.updates.lock().await;
        if self
            .store
            .configs()
            .await?
            .iter()
            .any(|c| c.enabled && c.dynamic_egress.is_some())
        {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::Conflict,
                "pause dynamic fetchers before editing instances",
            ));
        }
        self.dynamic_egress
            .as_ref()
            .ok_or_else(invalid)?
            .configure(config)
            .await
    }

    pub(crate) async fn configure(
        &self,
        config: TurnStateFetcherConfig,
    ) -> Result<(), ProviderStoreError> {
        ProviderAccountId::new(config.account_id.clone()).map_err(|_| invalid())?;
        if let Some(selection) = &config.dynamic_egress
            && config.enabled
        {
            if config.proxy_id.is_some() || !matches!(selection.family.as_str(), "ipv4" | "ipv6") {
                return Err(invalid());
            }
            let service = self.dynamic_egress.as_ref().ok_or_else(invalid)?;
            let status = service.status().await;
            if !status["instances"].as_array().is_some_and(|instances| {
                instances.iter().any(|instance| {
                    instance["id"].as_str() == Some(&selection.instance)
                        && instance["families"].as_array().is_some_and(|families| {
                            families
                                .iter()
                                .any(|family| family.as_str() == Some(&selection.family))
                        })
                })
            }) {
                return Err(invalid());
            }
        }
        if config.revision < 0
            || config.models.len() > 32
            || (config.enabled && config.models.is_empty())
            || config.models.iter().collect::<BTreeSet<_>>().len() != config.models.len()
            || config
                .models
                .iter()
                .any(|m| m.len() > 128 || UpstreamModelId::new(m.clone()).is_err())
        {
            return Err(invalid());
        }
        let _update = self.updates.lock().await;
        self.store.save_config(config.clone()).await?;
        if let Some(running) = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            && running.account == config.account_id
        {
            running.cancellation.cancel();
        }
        Ok(())
    }

    pub(crate) async fn enqueue(
        &self,
        account: &str,
        model: &str,
    ) -> Result<(), ProviderStoreError> {
        let _update = self.updates.lock().await;
        let config = self
            .store
            .configs()
            .await?
            .into_iter()
            .find(|c| c.account_id == account && c.enabled && c.models.iter().any(|m| m == model))
            .ok_or_else(invalid)?;
        if self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|r| r.account == account && r.model == model)
        {
            return Ok(());
        }
        let now = Utc::now().timestamp_millis();
        if self.store.attempts().await?.iter().any(|a| {
            a.account_id == account
                && a.model == model
                && a.config_revision == config.revision
                && (a.paused || (a.failures > 0 && a.next_attempt_at > now))
        }) {
            return Err(invalid());
        }
        self.store
            .save_attempt(TurnStateFetchAttempt {
                account_id: account.to_owned(),
                model: model.to_owned(),
                config_revision: config.revision,
                attempted_at: now,
                next_attempt_at: now,
                failures: 0,
                paused: false,
                status: "queued".to_owned(),
                message: "已排队".to_owned(),
                exit_ip: None,
                byte_length: None,
                duration_ms: 0,
                input_tokens: None,
                output_tokens: None,
            })
            .await
    }

    async fn persist(&self) -> Result<(), ProviderStoreError> {
        let values = self.cache.values();
        let changed = {
            let persisted = self
                .persisted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            values
                .iter()
                .filter(|value| !persisted.contains(value))
                .cloned()
                .collect::<Vec<_>>()
        };
        if !changed.is_empty() {
            self.store.save_values(changed).await?;
            *self
                .persisted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = values;
        }
        Ok(())
    }

    async fn cycle_inner(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProviderStoreError> {
        let _guard = self.cycle.lock().await;
        // 有界周期写回，不为每个正常请求生成后台任务；存储不可用时不继续付费获取。
        self.persist().await?;
        let configs = self.store.configs().await?;
        let attempts = self.store.attempts().await?;
        let values = self.cache.values();
        let now = Utc::now().timestamp_millis();
        let mut due = Vec::new();
        for config in configs.iter().filter(|c| c.enabled) {
            for model in &config.models {
                let previous = attempts.iter().find(|a| {
                    a.account_id == config.account_id
                        && a.model == *model
                        && a.config_revision == config.revision
                });
                if previous.is_some_and(|a| a.paused || a.next_attempt_at > now) {
                    continue;
                }
                let value = values
                    .iter()
                    .find(|v| v.account_id == config.account_id && v.model == *model);
                if previous.is_none_or(|a| a.status != "queued")
                    && value.is_some_and(|v| now < v.acquired_at.saturating_add(REFRESH_AFTER_MS))
                {
                    continue;
                }
                due.push((
                    previous.map_or(0, |a| a.next_attempt_at),
                    config.clone(),
                    model.clone(),
                    previous.cloned(),
                ));
            }
        }
        due.sort_by_key(|(at, _, _, _)| *at);
        let Some((_, config, model, previous)) = due.into_iter().next() else {
            return Ok(());
        };
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let stop = CancellationToken::new();
        *self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Running {
            account: config.account_id.clone(),
            model: model.clone(),
            cancellation: stop.clone(),
        });
        let started = Instant::now();
        let mut outcome = tokio::select! {
            () = cancellation.cancelled() => FetchOutcome::retry("服务正在停止", 60),
            () = stop.cancelled() => FetchOutcome::retry("配置已改变，本次获取已取消", 60),
            result = self.fetch_with_egress(&config, &model) => result,
        };
        *self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let _update = self.updates.lock().await;
        // 设置改变、暂停和删除后，旧任务不得写入共享缓存。
        if !self
            .store
            .configs()
            .await?
            .iter()
            .any(|c| c == &config && c.enabled)
        {
            return Ok(());
        }
        let finished = Utc::now().timestamp_millis();
        if let Some(value) = &outcome.value {
            self.cache.observe_at(
                &config.account_id,
                &model,
                value,
                "fetcher",
                outcome.received_at,
            );
            self.persist().await?;
            if !outcome.paused
                && outcome.retry_after == 0
                && self.cache.values().iter().any(|v| {
                    v.account_id == config.account_id
                        && v.model == model
                        && v.acquired_at + REFRESH_AFTER_MS > finished
                })
            {
                outcome.success = true;
                outcome.message = "已获得有效的 292 字节值".to_owned();
            } else if !outcome.paused && outcome.retry_after == 0 {
                outcome.message = "返回了相同旧值，未延长有效期".to_owned();
            }
        }
        let failures = if outcome.success {
            0
        } else {
            previous
                .as_ref()
                .map_or(1, |a| a.failures.saturating_add(1))
        };
        let delay = if outcome.success {
            0
        } else {
            backoff_seconds(failures).max(outcome.retry_after)
        };
        let next = if outcome.success {
            self.cache
                .values()
                .iter()
                .find(|v| v.account_id == config.account_id && v.model == model)
                .map_or(finished + REFRESH_AFTER_MS, |v| {
                    v.acquired_at + REFRESH_AFTER_MS
                })
        } else {
            finished
                .saturating_add(i64::try_from(delay.saturating_mul(1000)).unwrap_or(i64::MAX))
                .saturating_add(i64::from(uuid::Uuid::new_v4().as_bytes()[0]) * 20)
        };
        self.store
            .save_attempt(TurnStateFetchAttempt {
                account_id: config.account_id,
                model,
                config_revision: config.revision,
                attempted_at: now,
                next_attempt_at: next,
                failures,
                paused: outcome.paused,
                status: if outcome.success {
                    "success"
                } else if outcome.paused {
                    "paused"
                } else {
                    "retrying"
                }
                .to_owned(),
                message: outcome.message,
                byte_length: outcome.byte_length,
                duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                input_tokens: outcome.input_tokens,
                output_tokens: outcome.output_tokens,
                exit_ip: outcome.exit_ip,
            })
            .await
    }

    async fn fetch_with_egress(
        &self,
        config: &TurnStateFetcherConfig,
        model: &str,
    ) -> FetchOutcome {
        if config.dynamic_egress.is_some() {
            let Ok(id) = ProviderAccountId::new(config.account_id.clone()) else {
                return FetchOutcome::paused("账号无效");
            };
            let Ok(current) = self.accounts.load_current_credential(&id).await else {
                return FetchOutcome::paused("账号已删除或不可读取");
            };
            if !current.account.enabled() || current.account.quota().is_exhausted() {
                return FetchOutcome::retry("账号已禁用或额度已用尽，暂不申请新 IP", 900);
            }
            if matches!(
                current.account.credential_state(),
                CredentialState::Banned | CredentialState::Invalid
            ) {
                return FetchOutcome::paused("账号凭据不可用，请修复后保存配置重试");
            }
            if self.store.egress(config.clone()).await.is_err() {
                return FetchOutcome::retry("获取器配置已改变或不可用", 60);
            }
        }
        let lease = if let Some(selection) = &config.dynamic_egress {
            let Some(service) = &self.dynamic_egress else {
                return FetchOutcome::paused("未配置动态出口服务");
            };
            match service.acquire(selection).await {
                Ok(lease) => Some(lease),
                Err(()) => return FetchOutcome::retry("动态出口分配失败，未发送 OpenAI 请求", 60),
            }
        } else {
            None
        };
        // 申请出口可能耗时数分钟；进入 fetch 后重新读取账号、配置和调度状态。
        let mut outcome = tokio::time::timeout(
            Duration::from_secs(45),
            self.fetch(config, model, lease.as_ref()),
        )
        .await
        .unwrap_or_else(|_| FetchOutcome::retry("获取超时", 60));
        outcome.exit_ip = lease.as_ref().and_then(|lease| lease.ip.clone());
        outcome
    }

    async fn fetch(
        &self,
        config: &TurnStateFetcherConfig,
        model: &str,
        dynamic: Option<&super::dynamic_egress::Lease>,
    ) -> FetchOutcome {
        let Ok(id) = ProviderAccountId::new(config.account_id.clone()) else {
            return FetchOutcome::paused("账号无效");
        };
        let Ok(current) = self.accounts.load_current_credential(&id).await else {
            return FetchOutcome::paused("账号已删除或不可读取");
        };
        let account = &current.account;
        if !account.enabled() {
            return FetchOutcome::retry("账号已禁用，等待重新启用", 900);
        }
        if matches!(
            account.credential_state(),
            CredentialState::Banned | CredentialState::Invalid
        ) {
            return FetchOutcome::paused("账号凭据不可用，请修复后保存配置重试");
        }
        if account
            .access_token_expires_at()
            .is_some_and(|t| t <= SystemTime::now())
        {
            return FetchOutcome::retry("等待账号凭据刷新", 60);
        }
        if account.quota().is_exhausted() {
            return FetchOutcome::retry("账号额度已用尽，等待额度恢复", 900);
        }
        if !account.model_access().allows(model) {
            return FetchOutcome::paused("模型被账号限制，请修改配置后重试");
        }
        if self
            .catalog
            .cached_account_models(account)
            .ok()
            .flatten()
            .is_some_and(|models| !models.iter().any(|m| m == model))
        {
            return FetchOutcome::paused("账号模型目录不包含此模型");
        }
        let Ok(model_id) = UpstreamModelId::new(model.to_owned()) else {
            return FetchOutcome::paused("模型无效");
        };
        let scope = ProviderCooldownScope::upstream_model(model_id);
        let cooldown = self.cooldowns.read(&id).await;
        let scoped = self.cooldowns.read_scoped(&id, &scope).await;
        match (cooldown, scoped) {
            (Ok(account), Ok(model)) => {
                let until = account
                    .map(|v| v.until())
                    .into_iter()
                    .chain(model.map(|v| v.until()))
                    .max();
                if let Some(remaining) =
                    until.and_then(|t| t.duration_since(SystemTime::now()).ok())
                {
                    return FetchOutcome::retry(
                        "账号或模型正在冷却，延后获取",
                        remaining.as_secs().saturating_add(1),
                    );
                }
            }
            _ => return FetchOutcome::retry("账号调度状态暂不可用", 60),
        }
        let Ok(egress) = self.store.egress(config.clone()).await else {
            return FetchOutcome::paused("专用代理不可用或未通过测试，请检查后保存配置");
        };
        let Ok(runtime) = CodexCredentialCodec::decode(&current.credential) else {
            return FetchOutcome::paused("凭据格式无效");
        };
        let Ok(provider) = ProviderKind::new("openai") else {
            return FetchOutcome::paused("Provider 无效");
        };
        let Some(capacity) = NonZeroU32::new(egress.max_concurrent) else {
            return FetchOutcome::paused("账号并发配置无效");
        };
        let lease = self
            .leases
            .try_acquire(ProviderLeaseRequest::Scheduling(
                ProviderSchedulingLeaseRequest::new(
                    provider,
                    id.clone(),
                    account.revision(),
                    capacity,
                    Duration::from_millis(egress.request_interval_ms),
                    SystemTime::now() + Duration::from_secs(45),
                ),
            ))
            .await;
        let _lease = match lease {
            Ok(ProviderLeaseAcquisition::Acquired(guard)) => guard,
            _ => return FetchOutcome::retry("账号繁忙，延后获取", 60),
        };
        // 专用缓存键隔离 HTTP 连接池；代理未配置时显式直连，不使用环境代理。
        let http = if let Some(dynamic) = dynamic {
            let Some(http) = dynamic.http.clone() else {
                return FetchOutcome::retry("动态出口连接未就绪", 60);
            };
            http
        } else {
            let Ok(http) = build_account_http_client(
                &format!("turn-state-fetcher:{}", config.account_id),
                egress.proxy.as_ref(),
            ) else {
                return FetchOutcome::retry("专用连接初始化失败", 60);
            };
            http
        };
        let client = CodexBackendClient::new(http, &self.base_url, self.profile.clone())
            .with_base_url(runtime.openai_base_url.as_deref())
            .with_authentication(&runtime.authentication)
            .with_base_url(if dynamic.is_some() {
                Some(if runtime.authentication.oauth().is_some() {
                    crate::OFFICIAL_CODEX_BASE_URL
                } else {
                    "https://api.openai.com/v1"
                })
            } else {
                None
            });
        // 与账号连接测试及官方 Codex 一致，使用消息数组，不能依赖公开 API 的字符串简写。
        let mut body = json!({
            "model": model,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "Reply only OK."}]
            }],
            "instructions": "Be brief.",
            "stream": true,
            "store": false
        });
        if let Ok(Some(catalog)) = self.catalog.cached()
            && let Some(entry) = catalog
                .models()
                .iter()
                .find(|m| m.request_model().as_str() == model)
        {
            let efforts = entry.capabilities().reasoning_efforts();
            if let Some(effort) = ["none", "minimal", "low", "medium", "high"]
                .into_iter()
                .find(|e| efforts.iter().any(|v| v == e))
            {
                body["reasoning"] = json!({"effort":effort});
            }
        }
        // Codex OAuth 不统一支持 max_output_tokens；API Key Responses 接口支持时才发送。
        if runtime.authentication.oauth().is_none() {
            body["max_output_tokens"] = json!(32);
        }
        let Ok(payload) =
            ProtocolPayload::json_object("openai", body.as_object().cloned().unwrap_or_default())
        else {
            return FetchOutcome::paused("请求编码失败");
        };
        let payload = payload.with_context(
            json!({"use_websocket":false})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
        let mut request = match encode_generate_request(
            &GenerateRequest::from_protocol_payload(payload),
            model,
            None,
        ) {
            Ok(request) => request,
            Err(_) => return FetchOutcome::paused("请求编码失败"),
        };
        if runtime.authentication.oauth().is_none() {
            request
                .body_mut()
                .insert("max_output_tokens".to_owned(), json!(32));
        }
        let Ok(authorization) = runtime.authentication.authorization_header() else {
            return FetchOutcome::paused("凭据格式无效");
        };
        let request_id = format!("turn-state-fetch-{}", uuid::Uuid::new_v4());
        let context = CodexRequestContext::auxiliary(
            authorization.expose_secret(),
            account.upstream_account_id(),
            &request_id,
            Some(&runtime.installation_id),
        );
        let mut response = match client
            .create_response_stream_with_pool_account(&request, context, None)
            .await
        {
            Ok(response) => response,
            Err(error) => return FetchOutcome::from_error(&error),
        };
        let mut outcome = FetchOutcome::retry("未返回 292 字节值", 0);
        if let Some(value) = response.turn_state {
            outcome.capture(&value);
        }
        let mut decoder = SseEventDecoder::default();
        let mut bytes = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while let Ok(Some(chunk)) = tokio::time::timeout_at(deadline, response.body.next()).await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(_) => {
                    outcome.message = "响应流中断".to_owned();
                    break;
                }
            };
            bytes = bytes.saturating_add(chunk.len());
            if bytes > 1024 * 1024 {
                outcome.message = "响应超过获取器读取上限".to_owned();
                break;
            }
            let Ok(events) = decoder.push(&chunk) else {
                break;
            };
            let mut terminal = false;
            for event in events {
                let Ok(event) = serde_json::from_str::<Value>(&event.data) else {
                    continue;
                };
                if matches!(
                    event["type"].as_str(),
                    Some("response.completed" | "response.incomplete" | "response.failed")
                ) {
                    outcome.input_tokens = event
                        .pointer("/response/usage/input_tokens")
                        .and_then(Value::as_u64);
                    outcome.output_tokens = event
                        .pointer("/response/usage/output_tokens")
                        .and_then(Value::as_u64);
                    terminal = true;
                }
            }
            if terminal {
                break;
            }
        }
        // 防止获取期间发生凭据替换后将旧身份的结果用于新凭据。
        if !self
            .accounts
            .load_current_credential(&id)
            .await
            .is_ok_and(|fresh| fresh.account.revision() == account.revision())
        {
            return FetchOutcome::retry("凭据已更新，重新排队", 60);
        }
        outcome
    }
}

pub(crate) fn backoff_seconds(failures: u32) -> u64 {
    match failures {
        0 | 1 => 60,
        2 => 120,
        3 => 300,
        4 => 600,
        _ => 900,
    }
}

struct FetchOutcome {
    value: Option<String>,
    received_at: i64,
    success: bool,
    paused: bool,
    message: String,
    retry_after: u64,
    byte_length: Option<usize>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    exit_ip: Option<String>,
}
impl FetchOutcome {
    fn retry(message: &str, retry_after: u64) -> Self {
        Self {
            value: None,
            received_at: 0,
            success: false,
            paused: false,
            message: message.to_owned(),
            retry_after,
            byte_length: None,
            input_tokens: None,
            output_tokens: None,
            exit_ip: None,
        }
    }
    fn paused(message: &str) -> Self {
        let mut result = Self::retry(message, 900);
        result.paused = true;
        result
    }
    fn capture(&mut self, value: &str) {
        self.byte_length = Some(value.len());
        if super::turn_state::valid_value(value) {
            self.value = Some(value.to_owned());
            self.received_at = Utc::now().timestamp_millis();
        }
    }
    fn from_error(error: &CodexClientError) -> Self {
        if let Some(upstream) = error.upstream_failure() {
            let status = upstream.status.map(|s| s.as_u16());
            let mut result = if matches!(status, Some(401 | 403 | 404 | 400)) {
                Self::paused("上游拒绝请求，请检查账号、模型或网关配置")
            } else {
                Self::retry("上游请求失败", upstream.retry_after_seconds.unwrap_or(60))
            };
            result.message = fetch_failure_message(&upstream);
            if let CodexClientError::Upstream {
                client_response: Some(client_response),
                ..
            } = error
            {
                for (name, value) in client_response.client_headers() {
                    if name.eq_ignore_ascii_case("x-codex-turn-state")
                        && let Ok(value) = std::str::from_utf8(value)
                    {
                        result.capture(value);
                    }
                }
            }
            result
        } else {
            Self::retry("专用代理或上游连接失败", 60)
        }
    }
}

fn fetch_failure_message(failure: &CodexUpstreamFailure) -> String {
    let reason = match failure.category() {
        CodexFailureCategory::ModelUnsupported => "模型不可用或不受支持",
        CodexFailureCategory::CredentialExpired => "认证失败或凭据已过期",
        CodexFailureCategory::IdentityVerificationRequired => "账号需要身份验证",
        CodexFailureCategory::Banned => "账号或工作区被停用",
        CodexFailureCategory::UsageLimitExhausted => "当前用量窗口已耗尽",
        CodexFailureCategory::RateLimited => "请求受到限流",
        CodexFailureCategory::QuotaExhausted => "账号额度不足",
        CodexFailureCategory::CloudflareChallenge => "出口触发 Cloudflare 验证",
        CodexFailureCategory::CloudflarePathBlocked => "请求路径被 Cloudflare 拦截",
        CodexFailureCategory::InvalidRequest => "请求参数无效",
        CodexFailureCategory::PermissionDenied => "访问被拒绝，具体原因未识别",
        CodexFailureCategory::Timeout => "上游请求超时",
        CodexFailureCategory::CapacityUnavailable => "模型容量暂时不足",
        CodexFailureCategory::Unavailable => "上游不可用，具体原因未识别",
        CodexFailureCategory::Transport => "上游连接失败",
    };
    let status = failure.status.map_or_else(
        || "HTTP 状态未知".to_owned(),
        |s| format!("HTTP {}", s.as_u16()),
    );
    // 仅持久化固定分类与已有白名单错误码，原始正文可能回显凭据或请求内容。
    let code = failure
        .persistable_code()
        .map_or_else(String::new, |code| format!("；错误码：{code}"));
    format!("{status}：{reason}{code}")
}

impl ScheduledTask for TurnStateFetcher {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            self.cycle_inner(context.cancellation())
                .await
                .map_err(|_| WorkerTaskError::safe("Turn State fetcher persistence failed"))
        })
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use crate::transport::diagnostics::{CodexUpstreamDiagnostics, CodexUpstreamSendPhase};

    fn message(status: u16, body: &str) -> String {
        fetch_failure_message(&CodexUpstreamFailure::from_response(
            reqwest::StatusCode::from_u16(status).unwrap(),
            body,
            None,
            &CodexUpstreamDiagnostics::default(),
            None,
            &[],
            &[],
            CodexUpstreamSendPhase::AfterPayload,
        ))
    }

    #[test]
    fn fetch_diagnostics_preserve_status_and_safe_code_without_raw_secrets() {
        let result = message(
            401,
            r#"{"error":{"code":"token_expired","message":"Bearer private-token user@example.com"}}"#,
        );
        assert_eq!(
            result,
            "HTTP 401：认证失败或凭据已过期；错误码：token_expired"
        );
    }

    #[test]
    fn fetch_diagnostics_do_not_persist_unknown_codes_or_html() {
        for body in [
            r#"{"error":{"code":"private-token","message":"user@example.com"}}"#,
            "<html>private-token user@example.com</html>",
        ] {
            let result = message(403, body);
            assert!(result.starts_with("HTTP 403："));
            assert!(!result.contains("private-token"));
            assert!(!result.contains("user@example.com"));
            assert!(!result.contains("<html>"));
        }
    }
}

//! 独立低并发获取任务。凭据、账号额度与业务共用；连接池、出站代理与用量记录独立。

use super::turn_state::{REFRESH_AFTER_MS, REFRESH_BEFORE_MS, TurnStateCache};
use super::turn_state_probe_usage::{ProbeDiagnostics, ProbeUsage};
use crate::transport::canonical::{CodexCanonicalDecoder, CodexCanonicalOutcome};
use crate::{
    credential::{CodexCredentialCatalogService, CodexCredentialCodec},
    transport::{
        CodexBackendClient, CodexCatalogCapabilityEvidence, CodexClientError,
        client::{build_turn_state_http_client, turn_state_proxy},
        diagnostics::{CodexFailureCategory, CodexUpstreamFailure},
        encode_generate_request,
        profile::CodexWireProfileState,
    },
};
use chrono::Utc;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use gateway_core::{
    account::{CredentialState, ProviderAccountId, ProviderAccountStore},
    engine::ExecutionStore,
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
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime},
};

// 用随机种子初始化单调计数器：既保留随机性，也保证同一进程内并发探测不会生成重复前缀。
static PROBE_NONCE: OnceLock<Result<AtomicU64, ()>> = OnceLock::new();

fn next_probe_nonce() -> Result<u64, ()> {
    let counter = PROBE_NONCE
        .get_or_init(|| {
            let mut seed = [0_u8; 8];
            getrandom::fill(&mut seed).map_err(|_| ())?;
            Ok(AtomicU64::new(u64::from_le_bytes(seed)))
        })
        .as_ref()
        .map_err(|_| ())?;
    Ok(counter.fetch_add(1, Ordering::Relaxed))
}

/// 构造与 Codex Core 相同的 Responses 请求骨架。
///
/// 获取器没有真实对话历史，不能复制某个用户的 prompt；这里保留 Core 当前使用的
/// `additional_tools`、developer/user 消息、会话元数据和环境上下文形状，同时把正文
/// 控制在很小的范围内。随机数仍位于最后一条 user 消息最前面，确保每次探测都是新 turn。
fn build_probe_body(
    model: &str,
    nonce: u64,
    client_metadata: Value,
    responses_lite: bool,
) -> Value {
    let message_id = || format!("msg_{}", uuid::Uuid::new_v4());
    let namespace_tool = json!({
        "type": "namespace",
        "name": "functions",
        "description": "",
        "tools": [{
            "type": "function",
            "name": "probe_noop",
            "description": "Return a short acknowledgement.",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            },
            "strict": true
        }]
    });
    let input = json!([
        {
            "type": "additional_tools",
            "role": "developer",
            "id": format!("at_{}", uuid::Uuid::new_v4()),
            "tools": [namespace_tool]
        },
        {
            "type": "message",
            "role": "developer",
            "id": message_id(),
            "content": [{
                "type": "input_text",
                "text": "You are Codex, an AI coding agent."
            }]
        },
        {
            "type": "message",
            "role": "developer",
            "id": message_id(),
            "content": [{
                "type": "input_text",
                "text": "Keep this probe response short and do not call tools."
            }]
        },
        {
            "type": "message",
            "role": "user",
            "id": message_id(),
            "content": [{
                "type": "input_text",
                "text": "<environment_context><timezone>UTC</timezone></environment_context>"
            }]
        },
        {
            "type": "message",
            "role": "user",
            "id": message_id(),
            "content": [{
                "type": "input_text",
                "text": format!("{}\nReply only OK.", nonce)
            }]
        }
    ]);
    let mut body = json!({
        "model": model,
        "input": input,
        "tool_choice": "auto",
        "parallel_tool_calls": !responses_lite,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": client_metadata["session_id"],
        "client_metadata": client_metadata,
        "stream": true,
        "store": false
    });
    if !responses_lite {
        // API Key 走公开 Responses 接口，保留其兼容的 instructions/tools 形状；
        // OAuth 走 Codex Responses Lite，上面构造的 input 才是官方 Core 形状。
        body["instructions"] = json!("You are Codex, an AI coding agent.");
        body["tools"] = json!([]);
        body["input"] = json!([{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": format!("{}\nReply only OK.", nonce)
            }]
        }]);
    }
    body
}

struct Running {
    batch_id: String,
    account: String,
    model: String,
    cancellation: CancellationToken,
    group: String,
    initial_value: Option<String>,
}

struct RunningGuard<'a> {
    running: &'a Mutex<BTreeMap<uuid::Uuid, Running>>,
    id: uuid::Uuid,
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

#[derive(Clone)]
struct FetchPolicy {
    group: String,
    capacity: usize,
    interval: Duration,
}

fn fetch_policy(config: &TurnStateFetcherConfig, status: &Value) -> Option<FetchPolicy> {
    let Some(selection) = &config.dynamic_egress else {
        return Some(FetchPolicy {
            group: "static".to_owned(),
            capacity: 1,
            interval: Duration::ZERO,
        });
    };
    if status["available"] != true {
        return None;
    }
    let instance = status["instances"]
        .as_array()?
        .iter()
        .find(|i| i["id"].as_str() == Some(&selection.instance))?;
    let socks5 = matches!(instance["provider"].as_str(), Some("socks5" | "novaproxy"));
    Some(FetchPolicy {
        group: if socks5 {
            format!("socks5:{}", selection.instance)
        } else {
            "azure".to_owned()
        },
        capacity: if socks5 {
            usize::try_from(instance["maxConcurrent"].as_u64().unwrap_or(1).clamp(1, 16)).ok()?
        } else {
            1
        },
        interval: Duration::from_secs(instance["intervalSeconds"].as_u64().unwrap_or(10).min(3600)),
    })
}

pub(crate) struct TurnStateFetcher {
    store: Arc<dyn TurnStateStore>,
    accounts: Arc<dyn ProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    execution: Option<Arc<dyn ExecutionStore>>,
    catalog: Arc<CodexCredentialCatalogService>,
    selector: Arc<crate::credential::CodexCredentialSelector>,
    cache: TurnStateCache,
    profile: CodexWireProfileState,
    base_url: String,
    dynamic_egress: Option<super::dynamic_egress::DynamicEgress>,
    running: Mutex<BTreeMap<uuid::Uuid, Running>>,
    last_started: Mutex<BTreeMap<String, Instant>>,
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
        execution: Option<Arc<dyn ExecutionStore>>,
        catalog: Arc<CodexCredentialCatalogService>,
        selector: Arc<crate::credential::CodexCredentialSelector>,
        cache: TurnStateCache,
        profile: CodexWireProfileState,
        base_url: String,
    ) -> Self {
        cache.attach_store(store.clone());
        Self {
            store,
            accounts,
            leases,
            cooldowns,
            execution,
            catalog,
            selector,
            cache,
            profile,
            base_url,
            dynamic_egress: None,
            running: Mutex::new(BTreeMap::new()),
            last_started: Mutex::new(BTreeMap::new()),
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
        let running_requests: Vec<_> = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|r| (r.account.clone(), r.model.clone()))
            .collect();
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
            running: running_requests.first().cloned(),
            running_requests,
            recent_probes: self.store.recent_probes().await?,
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
        if config
            .schedule
            .as_ref()
            .is_some_and(|schedule| !schedule.is_valid())
        {
            return Err(invalid());
        }
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
        for running in self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|r| r.account == config.account_id)
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
            .values()
            .any(|r| r.account == account && r.model == model)
        {
            return Ok(());
        }
        let now = Utc::now().timestamp_millis();
        if self.store.attempts().await?.iter().any(|a| {
            a.account_id == account
                && a.model == model
                && a.config_revision == config.revision
                && (a.paused || (a.status == "cooldown" && a.next_attempt_at > now))
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
                search_concurrency: None,
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
        let status = match &self.dynamic_egress {
            Some(service) => service.status().await,
            None => Value::Null,
        };
        let mut tasks = FuturesUnordered::new();
        let cycle_started = Instant::now();
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            // 限制单个调度周期的准入时间，已开始的请求仍正常收尾。
            if cycle_started.elapsed() < Duration::from_secs(60) {
                for (config, model, policy) in self.due(&status).await? {
                    let id = uuid::Uuid::new_v4();
                    let stop = CancellationToken::new();
                    let batch_id = {
                        let mut running = self
                            .running
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let batch_id = running
                            .values()
                            .find(|r| {
                                r.account == config.account_id
                                    && r.model == model
                                    && !r.cancellation.is_cancelled()
                            })
                            .map_or_else(
                                || uuid::Uuid::new_v4().to_string(),
                                |r| r.batch_id.clone(),
                            );
                        running.insert(
                            id,
                            Running {
                                batch_id: batch_id.clone(),
                                account: config.account_id.clone(),
                                model: model.clone(),
                                cancellation: stop.clone(),
                                group: policy.group.clone(),
                                initial_value: self
                                    .cache
                                    .values()
                                    .iter()
                                    .find(|v| v.account_id == config.account_id && v.model == model)
                                    .map(|v| v.value.clone()),
                            },
                        );
                        batch_id
                    };
                    self.last_started
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(policy.group, Instant::now());
                    let guard = RunningGuard {
                        running: &self.running,
                        id,
                    };
                    tasks.push(async move {
                        let _guard = guard;
                        self.run_attempt(config, model, batch_id, stop, cancellation)
                            .await
                    });
                }
            }
            if tasks.is_empty() {
                return Ok(());
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                Some(result) = tasks.next() => result?,
                () = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }

    async fn due(
        &self,
        status: &Value,
    ) -> Result<Vec<(TurnStateFetcherConfig, String, FetchPolicy)>, ProviderStoreError> {
        // 有界周期写回，不为每个正常请求生成后台任务；存储不可用时不继续付费获取。
        self.persist().await?;
        let configs = self.store.configs().await?;
        let attempts = self.store.attempts().await?;
        let values = self.cache.values();
        let now = Utc::now().timestamp_millis();
        let mut due = Vec::new();
        for config in configs
            .iter()
            .filter(|c| c.enabled && c.allows_probe_at(now))
        {
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
                    && value.is_some_and(|v| now < v.expires_at.saturating_sub(REFRESH_BEFORE_MS))
                {
                    continue;
                }
                due.push((
                    previous.map_or(0, |a| a.next_attempt_at),
                    config.clone(),
                    model.clone(),
                ));
            }
        }
        let running = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for r in running.values() {
            if values.iter().any(|v| {
                v.account_id == r.account
                    && v.model == r.model
                    && v.expires_at.saturating_sub(REFRESH_BEFORE_MS) > now
                    && r.initial_value.as_ref() != Some(&v.value)
            }) {
                r.cancellation.cancel();
            }
        }
        due.sort_by_key(|(at, config, model)| {
            (
                running
                    .values()
                    .filter(|r| r.account == config.account_id && r.model == *model)
                    .count(),
                *at,
            )
        });
        let mut counts = BTreeMap::<String, usize>::new();
        for r in running.values() {
            *counts.entry(r.group.clone()).or_default() += 1;
        }
        let last = self
            .last_started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut selected = Vec::new();
        for (_, config, model) in due {
            let Some(policy) = fetch_policy(&config, status) else {
                continue;
            };
            if last
                .get(&policy.group)
                .is_some_and(|at| at.elapsed() < policy.interval)
            {
                continue;
            }
            let previous = attempts.iter().find(|a| {
                a.account_id == config.account_id
                    && a.model == model
                    && a.config_revision == config.revision
            });
            let model_capacity =
                if config.adaptive_concurrency && policy.group.starts_with("socks5:") {
                    previous
                        .and_then(|a| a.search_concurrency)
                        .unwrap_or(3)
                        .clamp(1, 8)
                        .min(policy.capacity)
                } else {
                    policy.capacity
                };
            let model_running = running
                .values()
                .filter(|r| r.account == config.account_id && r.model == model)
                .count();
            let count = counts.entry(policy.group.clone()).or_default();
            if *count < policy.capacity && model_running < model_capacity {
                selected.push((config, model, policy.clone()));
                // 非零间隔每轮只启动一个；零间隔在后续轮次补齐同一账号模型的槽位。
                *count = if policy.interval.is_zero() {
                    *count + 1
                } else {
                    policy.capacity
                };
            }
        }
        Ok(selected)
    }

    async fn run_attempt(
        &self,
        config: TurnStateFetcherConfig,
        model: String,
        batch_id: String,
        stop: CancellationToken,
        cancellation: &CancellationToken,
    ) -> Result<(), ProviderStoreError> {
        let now = Utc::now().timestamp_millis();
        if !config.allows_probe_at(now) {
            return Ok(());
        }
        let mut diagnostics = ProbeDiagnostics::new(
            self.store.clone(),
            TurnStateProbeRecord {
                id: uuid::Uuid::new_v4().to_string(),
                batch_id,
                account_id: config.account_id.clone(),
                model: model.clone(),
                config_revision: config.revision,
                profile: config.probe_profile,
                started_at: now,
                duration_ms: 0,
                outcome: "cancelled".to_owned(),
                http_status: None,
                http_version: None,
                byte_length: None,
                repeated: false,
                endpoint: None,
                responses_lite: false,
                compressed: false,
                egress_instance: config.dynamic_egress.as_ref().map(|v| v.instance.clone()),
                lease_id: None,
                exit_ip: None,
                fresh_connection: false,
            },
        );
        // 覆盖整个尝试（包括等待动态 IP）；时段结束后 Drop 负责释放在途租约。
        let window_end = async {
            if let Some(schedule) = &config.schedule {
                tokio::time::sleep(Duration::from_millis(schedule.remaining_ms(now))).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let initial_value = self
            .cache
            .values()
            .iter()
            .find(|v| v.account_id == config.account_id && v.model == model)
            .map(|v| v.value.clone());
        let started = Instant::now();
        let mut outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            () = stop.cancelled() => return Ok(()),
            () = window_end => return Ok(()),
            result = self.fetch_with_egress(&config, &model, diagnostics.record()) => result,
        };
        let _update = self.updates.lock().await;
        if stop.is_cancelled() {
            return Ok(());
        }
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
        diagnostics.record().outcome = "stale".to_owned();
        // 并发搜索或正常流量已获得新值时，迟到结果不能覆盖成功状态。
        if self.cache.values().iter().any(|v| {
            v.account_id == config.account_id
                && v.model == model
                && v.expires_at.saturating_sub(REFRESH_BEFORE_MS) > finished
                && initial_value.as_ref() != Some(&v.value)
        }) {
            return Ok(());
        }
        if let Some(value) = &outcome.value {
            diagnostics.record().repeated = self.cache.values().iter().any(|v| {
                v.account_id == config.account_id && v.model == model && v.value == *value
            });
            self.cache.observe_at(
                (&config.account_id, &model),
                value,
                "fetcher",
                outcome.proxy_url.as_deref(),
                outcome.session.as_ref(),
                outcome.received_at,
            );
            self.persist().await?;
            if !outcome.paused
                && outcome.retry_after == 0
                && self.cache.values().iter().any(|v| {
                    v.account_id == config.account_id
                        && v.model == model
                        && v.expires_at.saturating_sub(REFRESH_BEFORE_MS) > finished
                })
            {
                outcome.success = true;
                outcome.message = "已获得有效的 292 字节值".to_owned();
            } else if !outcome.paused && outcome.retry_after == 0 {
                outcome.message = "返回了相同旧值，未延长有效期".to_owned();
            }
        }
        let previous = self.store.attempts().await?.into_iter().find(|a| {
            a.account_id == config.account_id
                && a.model == model
                && a.config_revision == config.revision
        });
        let throttled = matches!(outcome.http_status, Some(429 | 503));
        // 命中、限流和不可恢复错误均收敛同组任务，迟到失败不得覆盖成功或冷却状态。
        if outcome.success || throttled || outcome.paused {
            for r in self
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
            {
                if r.account == config.account_id && r.model == model {
                    r.cancellation.cancel();
                }
            }
        }
        let failures = if outcome.success {
            0
        } else {
            previous
                .as_ref()
                .map_or(1, |a| a.failures.saturating_add(1))
        };
        let current_concurrency = previous
            .as_ref()
            .and_then(|a| a.search_concurrency)
            .unwrap_or(3);
        let search_concurrency = if outcome.success {
            3
        } else if throttled {
            1
        } else if outcome.byte_length == Some(312)
            && outcome.http_status.is_some_and(|s| (200..300).contains(&s))
        {
            match current_concurrency {
                0..=2 => 3,
                3..=4 => 5,
                _ => 8,
            }
        } else {
            current_concurrency
        };
        let delay = if throttled {
            outcome.retry_after.clamp(30, 3600)
        } else if outcome.success || outcome.paused || config.dynamic_egress.is_some() {
            0
        } else {
            10
        };
        let next = if outcome.success {
            self.cache
                .values()
                .iter()
                .find(|v| v.account_id == config.account_id && v.model == model)
                .map_or(finished + REFRESH_AFTER_MS, |v| {
                    v.expires_at.saturating_sub(REFRESH_BEFORE_MS)
                })
        } else {
            finished.saturating_add(i64::try_from(delay.saturating_mul(1000)).unwrap_or(i64::MAX))
        };
        diagnostics.record().outcome = if outcome.success {
            "captured"
        } else if throttled {
            "cooldown"
        } else if outcome.paused {
            "paused"
        } else if diagnostics.record().repeated {
            "repeated"
        } else if outcome.byte_length == Some(312) {
            "non_target"
        } else {
            "failed"
        }
        .to_owned();
        diagnostics.finish().await;
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
                } else if throttled {
                    "cooldown"
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
                search_concurrency: Some(search_concurrency),
            })
            .await
    }

    async fn fetch_with_egress(
        &self,
        config: &TurnStateFetcherConfig,
        model: &str,
        diagnostics: &mut TurnStateProbeRecord,
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
        let mut lease = if let Some(selection) = &config.dynamic_egress {
            let Some(service) = &self.dynamic_egress else {
                return FetchOutcome::paused("未配置动态出口服务");
            };
            match service.acquire(selection, config.probe_profile).await {
                Ok(lease) => Some(lease),
                Err(()) => return FetchOutcome::retry("动态出口分配失败，未发送 OpenAI 请求", 60),
            }
        } else {
            None
        };
        diagnostics.lease_id = lease.as_ref().map(|v| v.id.clone());
        diagnostics.exit_ip = lease.as_ref().and_then(|v| v.ip.clone());
        // 申请出口可能耗时数分钟；进入 fetch 后重新读取账号、配置和调度状态。
        let mut outcome = tokio::time::timeout(
            Duration::from_secs(45),
            self.fetch(config, model, lease.as_ref(), diagnostics),
        )
        .await
        .unwrap_or_else(|_| FetchOutcome::retry("获取超时", 60));
        outcome.exit_ip = lease.as_ref().and_then(|lease| lease.ip.clone());
        attach_probe_proxy(
            &mut outcome,
            lease
                .as_ref()
                .and_then(|lease| lease.upstream_proxy_url.as_deref()),
        );
        // 只保留成功响应中通过校验的票据；错误响应里的同长度头不能保留出口。
        if outcome.value.is_some()
            && let Some(lease) = lease.as_mut()
        {
            lease.retain_ip();
        }
        outcome
    }

    async fn fetch(
        &self,
        config: &TurnStateFetcherConfig,
        model: &str,
        dynamic: Option<&super::dynamic_egress::Lease>,
        diagnostics: &mut TurnStateProbeRecord,
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
        let oauth = runtime.authentication.oauth().is_some();
        let minimal = oauth && config.probe_profile == TurnStateProbeProfile::MinimalCompat;
        // 静态与动态出口都使用独立连接；明确未配置代理才允许直连。
        let http = if let Some(dynamic) = dynamic {
            let Some(http) = dynamic.http.clone() else {
                return FetchOutcome::retry("动态出口连接未就绪", 60);
            };
            http
        } else {
            let proxy = match egress
                .proxy
                .as_ref()
                .map(|p| turn_state_proxy(p.expose_url()))
                .transpose()
            {
                Ok(proxy) => proxy,
                Err(_) => return FetchOutcome::paused("专用代理配置无效"),
            };
            let Ok(http) = build_turn_state_http_client(proxy, minimal) else {
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
        let Ok(nonce) = next_probe_nonce() else {
            return FetchOutcome::retry("探测请求随机数生成失败", 60);
        };
        // Codex Core 每个 turn 都会带独立的会话、线程、窗口和根 turn 身份。获取器不
        // 复用真实对话，但保留同样的请求骨架，避免被上游按公开 Responses API 的简化
        // 请求处理。
        let root_turn_id = uuid::Uuid::new_v4().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();
        let thread_id = uuid::Uuid::new_v4().to_string();
        let window_id = uuid::Uuid::new_v4().to_string();
        let turn_id = uuid::Uuid::new_v4().to_string();
        let turn_started_at_unix_ms = Utc::now().timestamp_millis();
        let turn_metadata = json!({
            "installation_id": runtime.installation_id.clone(),
            "root_turn_id": root_turn_id.clone(),
            "session_id": session_id.clone(),
            "thread_id": thread_id.clone(),
            "window_id": window_id.clone(),
            "turn_id": turn_id.clone(),
            "request_kind": "turn",
            "turn_started_at_unix_ms": turn_started_at_unix_ms
        })
        .to_string();
        let responses_lite = oauth && !minimal;
        diagnostics.profile = if minimal {
            TurnStateProbeProfile::MinimalCompat
        } else {
            TurnStateProbeProfile::CodexCore
        };
        diagnostics.responses_lite = responses_lite;
        diagnostics.compressed = oauth && !minimal;
        diagnostics.endpoint = Some(
            if oauth {
                "/codex/responses"
            } else {
                "/responses"
            }
            .to_owned(),
        );
        let mut body = build_probe_body(
            model,
            nonce,
            json!({
                "root_turn_id": root_turn_id,
                "x-codex-installation-id": runtime.installation_id,
                "session_id": session_id,
                "thread_id": thread_id,
                "x-codex-window-id": window_id,
                "turn_id": turn_id,
                "x-codex-turn-metadata": turn_metadata
            }),
            responses_lite,
        );
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
                let mut reasoning = json!({"effort":effort,"summary":"auto"});
                if responses_lite {
                    reasoning["context"] = json!("all_turns");
                }
                body["reasoning"] = reasoning;
            }
            if entry.capabilities().parallel_tool_calls()
                == CodexCatalogCapabilityEvidence::DeclaredUnsupported
            {
                body.as_object_mut()
                    .expect("probe body is an object")
                    .remove("parallel_tool_calls");
            }
            if entry.capabilities().verbosity() == CodexCatalogCapabilityEvidence::DeclaredNative {
                body["text"] = json!({"verbosity":"low"});
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
        let mut payload_context = Map::new();
        payload_context.insert("use_websocket".to_owned(), Value::Bool(false));
        if responses_lite {
            payload_context.insert(
                "responses_lite".to_owned(),
                Value::String("true".to_owned()),
            );
        }
        let payload = payload.with_context(payload_context);
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
        let request_id = format!("req_turn_state_fetch_{}", uuid::Uuid::new_v4());
        let Ok(response_origin) = client.request_url(crate::transport::CODEX_RESPONSES_PATH) else {
            return FetchOutcome::paused("请求地址无效");
        };
        let cookies = self
            .selector
            .replay_cookies(runtime.cookies, &response_origin);
        let Ok(cookie_header) = super::observation::build_cookie_header(&cookies) else {
            return FetchOutcome::paused("账号 Cookie 无效");
        };
        let mut usage = ProbeUsage::start(
            self.execution.clone(),
            &request_id,
            &id,
            model,
            egress.config_revision,
            if minimal {
                None
            } else {
                request
                    .body()
                    .get("reasoning")
                    .and_then(|value| value.get("effort"))
                    .and_then(Value::as_str)
            },
        )
        .await;
        let mut context = super::observation::codex_request_context(
            &request,
            &request_id,
            account,
            &runtime.installation_id,
            &authorization,
            cookie_header.as_ref(),
            Default::default(),
        );
        // 将探测会话身份投影到协议头；成功后随票据保存，供自动模式延续会话。
        context.session_id = Some(&session_id);
        context.thread_id = Some(&thread_id);
        context.codex_window_id = Some(&window_id);
        context.turn_id = Some(&turn_id);
        context.turn_metadata = Some(&turn_metadata);
        diagnostics.fresh_connection = true;
        let result = if minimal {
            client.create_minimal_turn_state_probe(model, context).await
        } else {
            client
                .create_response_stream_with_pool_account(&request, context, None)
                .await
        };
        let mut response = match result {
            Ok(response) => response,
            Err(error) => {
                let outcome = FetchOutcome::from_error(&error);
                diagnostics.http_status = outcome.http_status;
                diagnostics.byte_length = outcome.byte_length;
                if let CodexClientError::Upstream {
                    transport_metrics, ..
                } = &error
                {
                    diagnostics.http_version = transport_metrics.http_version.clone();
                }
                usage.opening_error(&error);
                usage.finish().await;
                return outcome;
            }
        };
        let mut outcome = FetchOutcome::retry("未返回 292 字节值", 0);
        // 探测和正式流量共用 Cookie 域、路径及凭据版本校验，不重放请求级身份头。
        let mut credential_revision = account.revision().get();
        if !response.set_cookie_headers.is_empty() {
            match self
                .selector
                .capture_response_cookies(account, &response_origin, &response.set_cookie_headers)
                .await
            {
                Ok(captured) => {
                    credential_revision =
                        captured.credential_revision.unwrap_or(credential_revision);
                }
                Err(_) => {
                    usage.response(&response);
                    usage.fail("probe cookie persistence failed");
                    usage.finish().await;
                    return FetchOutcome::retry("探测 Cookie 保存失败", 60);
                }
            }
        }
        usage.response(&response);
        outcome.http_status = response.diagnostics.status_code;
        diagnostics.http_status = outcome.http_status;
        diagnostics.http_version = response.transport_metrics.http_version.clone();
        if let Some(value) = response.turn_state {
            outcome.capture(&value);
            outcome.session = outcome.value.as_ref().map(|_| TurnStateSession {
                session_id,
                thread_id,
                window_id,
            });
            diagnostics.byte_length = outcome.byte_length;
        }
        if dynamic.is_none() {
            attach_probe_proxy(
                &mut outcome,
                egress.proxy.as_ref().map(|proxy| proxy.expose_url()),
            );
        }
        let mut decoder = CodexCanonicalDecoder::new(model)
            .with_reported_model(response.response_metadata.effective_model.as_deref());
        let mut bytes = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let chunk = match tokio::time::timeout_at(deadline, response.body.next()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(None) => {
                    usage.decoded(decoder.finish());
                    if !usage.completed() {
                        usage.fail("probe stream ended before a terminal response");
                        outcome.message = "响应流未完整结束".to_owned();
                    }
                    break;
                }
                Ok(Some(Err(_))) => {
                    usage.fail("probe response stream interrupted");
                    outcome.message = "响应流中断".to_owned();
                    break;
                }
                Err(_) => {
                    usage.timeout();
                    outcome.message = "探测响应读取超时".to_owned();
                    break;
                }
            };
            bytes = bytes.saturating_add(chunk.len());
            if bytes > 1024 * 1024 {
                usage.fail("probe response exceeded read limit");
                outcome.message = "响应超过获取器读取上限".to_owned();
                break;
            }
            let decoded = decoder.push(&chunk);
            let failed = matches!(decoded, CodexCanonicalOutcome::Failed(_));
            usage.decoded(decoded);
            if failed {
                outcome.message = "上游返回探测失败事件".to_owned();
                break;
            }
            if usage.completed() {
                break;
            }
        }
        usage.service_tier(decoder.response_service_tier());
        outcome.input_tokens = usage.tokens().input_tokens;
        outcome.output_tokens = usage.tokens().output_tokens;
        usage.finish().await;
        // 防止获取期间发生凭据替换后将旧身份的结果用于新凭据。
        if !self
            .accounts
            .load_current_credential(&id)
            .await
            .is_ok_and(|fresh| fresh.account.revision().get() == credential_revision)
        {
            outcome.value = None;
            outcome.received_at = 0;
            outcome.success = false;
            outcome.retry_after = 60;
            outcome.message = "凭据已更新，重新排队".to_owned();
        }
        outcome
    }
}

#[cfg(test)]
mod probe_nonce_tests {
    use super::next_probe_nonce;

    #[test]
    fn probe_nonce_is_numeric_and_unique() {
        let first = next_probe_nonce().unwrap();
        let second = next_probe_nonce().unwrap();
        assert_ne!(first, second);
    }
}

#[cfg(test)]
mod probe_body_tests {
    use super::build_probe_body;

    #[test]
    fn probe_body_uses_codex_core_message_shape_without_user_history() {
        let body = build_probe_body(
            "gpt-5.4",
            123456,
            serde_json::json!({
                "root_turn_id": "root-turn",
                "x-codex-installation-id": "installation",
                "session_id": "session",
                "thread_id": "thread",
                "x-codex-window-id": "window",
                "turn_id": "turn",
                "x-codex-turn-metadata": r#"{"request_kind":"turn"}"#
            }),
            true,
        );

        assert!(body.get("instructions").is_none());
        assert!(body.get("tools").is_none());
        assert_eq!(body["model"], "gpt-5.4");
        assert_eq!(body["input"].as_array().map(Vec::len), Some(5));
        assert_eq!(body["input"][0]["type"], "additional_tools");
        assert_eq!(body["input"][0]["role"], "developer");
        assert_eq!(body["input"][0]["id"].as_str().map(str::len), Some(39));
        assert_eq!(body["input"][0]["tools"][0]["type"], "namespace");
        assert_eq!(body["parallel_tool_calls"], false);
        assert_eq!(body["input"][1]["role"], "developer");
        assert_eq!(body["input"][3]["role"], "user");
        assert_eq!(
            body["input"][4]["content"][0]["text"],
            "123456\nReply only OK."
        );
        assert_eq!(body["client_metadata"]["root_turn_id"], "root-turn");
        assert_eq!(
            body["client_metadata"]["x-codex-installation-id"],
            "installation"
        );
        assert_eq!(
            body["client_metadata"]["x-codex-turn-metadata"],
            r#"{"request_kind":"turn"}"#
        );

        let api_key_body = build_probe_body(
            "gpt-5.4",
            123456,
            serde_json::json!({
                "root_turn_id": "root-turn",
                "x-codex-installation-id": "installation",
                "session_id": "session",
                "thread_id": "thread",
                "x-codex-window-id": "window",
                "turn_id": "turn",
                "x-codex-turn-metadata": r#"{"request_kind":"turn"}"#
            }),
            false,
        );
        assert_eq!(
            api_key_body["instructions"],
            "You are Codex, an AI coding agent."
        );
        assert_eq!(api_key_body["tools"], serde_json::json!([]));
        assert_eq!(api_key_body["parallel_tool_calls"], true);
    }
}

struct FetchOutcome {
    http_status: Option<u16>,
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
    proxy_url: Option<String>,
    session: Option<TurnStateSession>,
}
fn attach_probe_proxy(outcome: &mut FetchOutcome, proxy_url: Option<&str>) {
    if outcome.value.is_some()
        && let Some(proxy_url) = proxy_url
    {
        outcome.proxy_url = Some(proxy_url.to_owned());
    }
}

impl FetchOutcome {
    fn retry(message: &str, retry_after: u64) -> Self {
        Self {
            http_status: None,
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
            proxy_url: None,
            session: None,
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
            result.http_status = status;
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
                        // 错误响应仅记录长度，不将票据写入自动缓存。
                        result.byte_length = Some(value.len());
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

#[cfg(test)]
mod probe_proxy_tests {
    use super::{FetchOutcome, attach_probe_proxy};

    #[test]
    fn only_valid_captured_value_receives_probe_proxy() {
        let mut captured = FetchOutcome::retry("pending", 0);
        captured.capture(&"a".repeat(292));
        attach_probe_proxy(&mut captured, Some("socks5h://probe"));
        assert_eq!(captured.proxy_url.as_deref(), Some("socks5h://probe"));

        let mut non_target = FetchOutcome::retry("pending", 0);
        non_target.capture(&"b".repeat(291));
        attach_probe_proxy(&mut non_target, Some("socks5h://probe"));
        assert_eq!(non_target.proxy_url, None);
    }
}

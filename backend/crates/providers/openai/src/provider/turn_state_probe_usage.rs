//! 获取器的内部消费记录：沿用执行存储与 canonical 用量，取消时也收敛已开始的请求。

use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use gateway_core::{
    account::ProviderAccountId,
    engine::{
        AttemptRecord, AttemptTrigger, ExecutionOutcome, ExecutionStore,
        ModelRequestFailureObservation, ModelRequestFinalization, ModelRequestId,
        ModelRequestTimings, NewModelRequest, provider::ContinuationRequestObservation,
    },
    error::{GatewayError, GatewayErrorKind},
    event::{GatewayEvent, ProviderEvent},
    metering::{CostEstimate, CostSource, Usage},
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{
        AccountRoutingSnapshot, ConfigRevision, ProviderKind, PublicModelId, UpstreamModelId,
    },
    upstream::UpstreamSendState,
};

use super::observation::{actual_transport_name, turn_state_metadata};
use crate::transport::{
    CodexBackendStreamingResponse, CodexClientError, CodexUpstreamSendPhase,
    canonical::CodexCanonicalOutcome,
};

pub(super) struct ProbeUsage {
    store: Option<Arc<dyn ExecutionStore>>,
    finalization: Option<ModelRequestFinalization>,
    started: Instant,
}

impl ProbeUsage {
    pub(super) async fn start(
        mut store: Option<Arc<dyn ExecutionStore>>,
        request_id: &str,
        account_id: &ProviderAccountId,
        model: &str,
        config_revision: ConfigRevision,
        reasoning_effort: Option<&str>,
    ) -> Self {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let id = ModelRequestId::new(request_id).expect("generated probe request ID");
        let request = NewModelRequest {
            id: id.clone(),
            client_api_key_id: None,
            client_api_key_ref: ClientApiKeyId::new("system:turn-state-fetcher")
                .expect("internal key ref"),
            config_revision,
            routing: AccountRoutingSnapshot::all(),
            protocol: "openai".to_owned(),
            operation: OperationKind::Generate,
            endpoint: "/v1/responses".to_owned(),
            client_transport: "internal".to_owned(),
            requested_model: Some(PublicModelId::new(model).expect("validated probe model")),
            client_ip: None,
            user_agent: Some("codex-proxy-rs/turn-state-fetcher".to_owned()),
            reasoning_effort: reasoning_effort.map(str::to_owned),
            reasoning_preset: None,
            request_kind: Some("turn_state_fetcher".to_owned()),
            subagent_kind: None,
            compact: false,
            continuation: ContinuationRequestObservation::default(),
            image_generation_requested: false,
            admission_decision_ms: None,
            started_at,
            deadline_at: started_at + Duration::from_secs(60),
        };
        let attempt = AttemptRecord {
            request_id: id.clone(),
            attempt_count: NonZeroU32::MIN,
            trigger: AttemptTrigger::Initial,
            provider_kind: ProviderKind::new("openai").expect("OpenAI provider"),
            provider_account_id: Some(account_id.clone()),
            provider_account_ref: Some(account_id.clone()),
            upstream_model_id: Some(UpstreamModelId::new(model).expect("validated probe model")),
            upstream_transport: "http_sse".to_owned(),
            http_version: None,
            account_selection_wait_ms: None,
            capacity_used_slots: None,
            capacity_total_slots: None,
        };
        if let Some(execution) = &store
            && let Err(error) = execution
                .create_model_request_with_attempt(request, attempt)
                .await
        {
            tracing::warn!(?error, request_id, "写入 292 探测起始观测失败");
            store = None;
        }
        Self {
            store,
            started,
            finalization: Some(ModelRequestFinalization {
                request_id: id,
                outcome: ExecutionOutcome::Cancelled,
                send_state: UpstreamSendState::Ambiguous,
                attempt_count: 1,
                downstream_committed_at: None,
                client_status_code: None,
                client_response_id: None,
                upstream_status_code: None,
                upstream_request_id: None,
                upstream_response_id: None,
                upstream_transport: Some("http_sse".to_owned()),
                http_version: None,
                websocket_pool: None,
                service_tier: None,
                upstream_response_model: None,
                provider_metadata_json: None,
                diagnostic_trace_json: None,
                error: Some(GatewayError::new(
                    GatewayErrorKind::Cancelled,
                    "turn state probe cancelled",
                )),
                provider_error_code: None,
                raw_upstream_error: None,
                failure_observation: ModelRequestFailureObservation::default(),
                retry_after_ms: None,
                usage: Usage::new(),
                image_generation_succeeded: None,
                cost: CostEstimate::unavailable(),
                timings: ModelRequestTimings::default(),
                completed_at: started_at,
            }),
        }
    }

    fn record(&mut self) -> &mut ModelRequestFinalization {
        self.finalization
            .as_mut()
            .expect("probe observation not finalized")
    }

    pub(super) fn response(&mut self, response: &CodexBackendStreamingResponse) {
        let headers_ms = self.elapsed_ms();
        let record = self.record();
        record.send_state = UpstreamSendState::Sent;
        record.upstream_status_code = response.diagnostics.status_code;
        record.upstream_request_id = response.diagnostics.request_id.clone();
        record.upstream_transport = Some(actual_transport_name(response.transport).to_owned());
        record.http_version = response.transport_metrics.http_version.clone();
        record.upstream_response_model = response.response_metadata.effective_model.clone();
        record.timings.headers_ms = Some(headers_ms);
        record.provider_metadata_json =
            turn_state_metadata(&response.response_metadata.client_headers)
                .map(|metadata| metadata.as_json().to_owned());
    }

    pub(super) fn opening_error(&mut self, error: &CodexClientError) {
        self.fail("turn state probe upstream request failed");
        let record = self.record();
        if let Some(transport) = error.transport() {
            record.upstream_transport = Some(actual_transport_name(transport).to_owned());
        }
        if let Some(failure) = error.upstream_failure() {
            record.upstream_status_code = failure.status.map(|status| status.as_u16());
            record.upstream_request_id = failure.request_id.clone();
            record.provider_error_code = failure.persistable_code().map(str::to_owned);
            record.send_state = match failure.send_phase {
                CodexUpstreamSendPhase::BeforePayload => UpstreamSendState::NotSent,
                CodexUpstreamSendPhase::AfterPayload => UpstreamSendState::Sent,
                CodexUpstreamSendPhase::Ambiguous => UpstreamSendState::Ambiguous,
            };
        } else if let CodexClientError::Http(error) = error
            && error.is_connect()
        {
            record.send_state = UpstreamSendState::NotSent;
        }
        if let CodexClientError::Upstream {
            client_response: Some(response),
            ..
        } = error
        {
            record.provider_metadata_json = turn_state_metadata(response.client_headers())
                .map(|metadata| metadata.as_json().to_owned());
        }
    }

    pub(super) fn decoded(&mut self, outcome: CodexCanonicalOutcome) {
        match outcome {
            CodexCanonicalOutcome::Events(events) => self.events(&events),
            CodexCanonicalOutcome::Failed(failure) => {
                self.events(failure.events());
                self.fail("turn state probe returned a failed response");
            }
        }
    }

    fn events(&mut self, events: &[ProviderEvent]) {
        let elapsed_ms = self.elapsed_ms();
        for event in events {
            for fact in event.canonical_facts() {
                let record = self.record();
                record.timings.first_event_ms.get_or_insert(elapsed_ms);
                match fact {
                    GatewayEvent::Usage(usage) => record.usage.merge(usage),
                    GatewayEvent::CalculatedCost(cost)
                        if record.cost.source() != CostSource::ProviderReported =>
                    {
                        record.cost = cost.clone().into_estimate()
                    }
                    GatewayEvent::ProviderCost(cost) => record.cost = (*cost).into_estimate(),
                    GatewayEvent::Started(meta) | GatewayEvent::Completed(meta) => {
                        record.upstream_response_id = Some(meta.response_id().to_owned());
                        record.upstream_response_model = meta.model().map(str::to_owned);
                        if matches!(fact, GatewayEvent::Completed(_)) {
                            record.outcome = ExecutionOutcome::Succeeded;
                            record.send_state = UpstreamSendState::Sent;
                            // 内部消费者完整消费终态，即为该请求的提交边界；不向客户 Key 结算。
                            record.downstream_committed_at = Some(SystemTime::now());
                            record.client_status_code = Some(200);
                            record.error = None;
                        }
                    }
                    GatewayEvent::TextDelta(_) | GatewayEvent::ReasoningDelta(_) => {
                        record.timings.first_token_ms.get_or_insert(elapsed_ms);
                    }
                    _ => {}
                }
            }
        }
    }

    pub(super) fn service_tier(&mut self, tier: Option<&str>) {
        self.record().service_tier = tier.map(str::to_owned);
    }

    pub(super) fn completed(&self) -> bool {
        self.finalization
            .as_ref()
            .is_some_and(|record| record.outcome == ExecutionOutcome::Succeeded)
    }

    pub(super) fn tokens(&self) -> &Usage {
        &self
            .finalization
            .as_ref()
            .expect("probe observation not finalized")
            .usage
    }

    pub(super) fn fail(&mut self, message: &'static str) {
        let record = self.record();
        record.outcome = ExecutionOutcome::Failed;
        record.downstream_committed_at = None;
        record.client_status_code = None;
        record.error = Some(GatewayError::new(
            GatewayErrorKind::UpstreamUnavailable,
            message,
        ));
    }

    pub(super) fn timeout(&mut self) {
        self.fail("turn state probe timed out");
        self.record().error = Some(GatewayError::new(
            GatewayErrorKind::Timeout,
            "turn state probe timed out",
        ));
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn take_finalization(&mut self) -> Option<ModelRequestFinalization> {
        let mut record = self.finalization.take()?;
        record.completed_at = SystemTime::now();
        record.timings.latency_ms = Some(self.elapsed_ms());
        Some(record)
    }

    pub(super) async fn finish(mut self) {
        if let Some(record) = self.take_finalization()
            && let Some(store) = &self.store
        {
            persist(store.as_ref(), record).await;
        }
    }
}

impl Drop for ProbeUsage {
    fn drop(&mut self) {
        // 配置修改、并发命中和外层超时都会丢弃 fetch future；已发送请求仍须留下终态。
        // 正常路径已取走终态，Drop 不会重复写入；停机时未能排空的 running 行由 deadline 恢复。
        if let Some(record) = self.take_finalization()
            && let Some(store) = self.store.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move { persist(store.as_ref(), record).await });
        }
    }
}

async fn persist(store: &dyn ExecutionStore, record: ModelRequestFinalization) {
    if let Err(error) = store.finalize_model_request(record).await {
        tracing::warn!(?error, "写入 292 探测终态观测失败");
    }
}

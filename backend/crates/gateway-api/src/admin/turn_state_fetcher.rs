//! 独立 Turn State 获取器管理接口。
use super::{
    AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminResponse, wire::map_admin_service_error,
};
use crate::auth::SessionState;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
pub fn router<S>() -> Router<S>
where
    S: SessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/api/admin/turn-state-fetcher/egress",
            post(configure_dynamic_egress::<S>),
        )
        .route(
            "/api/admin/turn-state-fetcher",
            get(turn_state_fetcher::<S>),
        )
        .route(
            "/api/admin/turn-state-fetcher/configure",
            post(configure_turn_state_fetcher::<S>),
        )
        .route(
            "/api/admin/turn-state-fetcher/run",
            post(run_turn_state_fetcher::<S>),
        )
}

async fn configure_dynamic_egress<S>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminJson(config): AdminJson<serde_json::Value>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    state
        .admin_services()
        .openai()
        .configure_dynamic_egress(config)
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(())))
}

async fn turn_state_fetcher<S>(
    _auth: AdminAuth,
    State(state): State<S>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let data = state
        .admin_services()
        .openai()
        .turn_state_fetcher()
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

async fn configure_turn_state_fetcher<S>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminJson(config): AdminJson<gateway_core::provider_ports::turn_state::TurnStateFetcherConfig>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    state
        .admin_services()
        .openai()
        .configure_turn_state_fetcher(config)
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(())))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RunTurnStateFetcher {
    account_id: String,
    model: String,
}

async fn run_turn_state_fetcher<S>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<RunTurnStateFetcher>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    state
        .admin_services()
        .openai()
        .run_turn_state_fetcher(&request.account_id, &request.model)
        .await
        .map_err(map_admin_service_error)?;
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(())))
}

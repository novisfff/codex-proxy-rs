//! 获取器配置、有效状态和最近尝试；更新以配置 revision 防止旧任务覆盖新设置。

use futures::future::BoxFuture;
use gateway_core::provider_ports::{ProviderStoreError, ProviderStoreErrorKind, turn_state::*};
use sqlx::{PgPool, Row};

pub struct PgTurnStateStore(PgPool);
impl PgTurnStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
}

fn unavailable(_: sqlx::Error) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, "turn state")
}
fn invalid() -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, "turn state")
}

impl TurnStateStore for PgTurnStateStore {
    fn recent_probes(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TurnStateProbeRecord>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(sqlx::query_scalar::<_, sqlx::types::Json<TurnStateProbeRecord>>(
                "SELECT data FROM turn_state_probe_history ORDER BY started_at DESC, id DESC LIMIT 200",
            ).fetch_all(&self.0).await.map_err(unavailable)?.into_iter().map(|v| v.0).collect())
        })
    }

    fn save_probe(
        &self,
        probe: TurnStateProbeRecord,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let mut tx = self.0.begin().await.map_err(unavailable)?;
            // 多实例写入共用短事务锁，确保裁剪后的历史始终有界。
            sqlx::query("SELECT pg_advisory_xact_lock(2920022)")
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            sqlx::query("INSERT INTO turn_state_probe_history(id,account_id,started_at,data) SELECT $1,$2,$3,$4 WHERE EXISTS(SELECT 1 FROM turn_state_fetcher_configs WHERE account_id=$2) ON CONFLICT(id) DO NOTHING")
                .bind(&probe.id).bind(&probe.account_id).bind(probe.started_at).bind(sqlx::types::Json(&probe))
                .execute(&mut *tx).await.map_err(unavailable)?;
            sqlx::query("DELETE FROM turn_state_probe_history WHERE id IN (SELECT id FROM turn_state_probe_history ORDER BY started_at DESC, id DESC OFFSET 200)")
                .execute(&mut *tx).await.map_err(unavailable)?;
            tx.commit().await.map_err(unavailable)
        })
    }

    fn configs(&self) -> BoxFuture<'_, Result<Vec<TurnStateFetcherConfig>, ProviderStoreError>> {
        Box::pin(async move {
            sqlx::query("SELECT account_id, enabled, models, proxy_id, dynamic_egress, schedule, probe_profile, adaptive_concurrency, refresh_interval_minutes, state_ttl_minutes, revision FROM turn_state_fetcher_configs ORDER BY account_id")
                .fetch_all(&self.0).await.map_err(unavailable)?.into_iter().map(|row| Ok(TurnStateFetcherConfig {
                    account_id: row.try_get("account_id").map_err(unavailable)?,
                    enabled: row.try_get("enabled").map_err(unavailable)?,
                    models: row.try_get::<sqlx::types::Json<Vec<String>>, _>("models").map_err(unavailable)?.0,
                    proxy_id: row.try_get("proxy_id").map_err(unavailable)?,
                    dynamic_egress: row.try_get::<Option<sqlx::types::Json<DynamicEgressSelection>>, _>("dynamic_egress").map_err(unavailable)?.map(|v| v.0),
                    schedule: row.try_get::<Option<sqlx::types::Json<TurnStateFetcherSchedule>>, _>("schedule").map_err(unavailable)?.map(|v| v.0),
                    probe_profile: match row.try_get::<String, _>("probe_profile").map_err(unavailable)?.as_str() {
                        "codex_core" => TurnStateProbeProfile::CodexCore,
                        "minimal_compat" => TurnStateProbeProfile::MinimalCompat,
                        _ => return Err(invalid()),
                    },
                    adaptive_concurrency: row.try_get("adaptive_concurrency").map_err(unavailable)?,
                    refresh_interval_minutes: u16::try_from(row.try_get::<i32, _>("refresh_interval_minutes").map_err(unavailable)?).map_err(|_| invalid())?,
                    state_ttl_minutes: u16::try_from(row.try_get::<i32, _>("state_ttl_minutes").map_err(unavailable)?).map_err(|_| invalid())?,
                    revision: row.try_get("revision").map_err(unavailable)?,
                })).collect()
        })
    }

    fn save_config(
        &self,
        config: TurnStateFetcherConfig,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            if !config.valid_refresh_interval()
                || (config.dynamic_egress.is_some() && config.proxy_id.is_some())
                || config
                    .schedule
                    .as_ref()
                    .is_some_and(|schedule| !schedule.is_valid())
            {
                return Err(invalid());
            }
            let mut tx = self.0.begin().await.map_err(unavailable)?;
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM provider_accounts WHERE id=$1 AND provider_kind='openai')")
                .bind(&config.account_id).fetch_one(&mut *tx).await.map_err(unavailable)?;
            if !exists {
                return Err(invalid());
            }
            if let Some(id) = &config.proxy_id {
                let proxy = sqlx::query(
                    "SELECT last_test_success FROM outbound_proxies WHERE id=$1 FOR SHARE",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?
                .ok_or_else(invalid)?;
                if config.enabled
                    && proxy
                        .try_get::<Option<bool>, _>("last_test_success")
                        .map_err(unavailable)?
                        != Some(true)
                {
                    return Err(invalid());
                }
            }
            let updated = sqlx::query("INSERT INTO turn_state_fetcher_configs(account_id,enabled,models,proxy_id,dynamic_egress,schedule,probe_profile,adaptive_concurrency,refresh_interval_minutes,state_ttl_minutes) SELECT $1,$2,$3,$4,$6,$7,$8,$9,$10,$11 WHERE $5=0 ON CONFLICT DO NOTHING")
                .bind(&config.account_id).bind(config.enabled).bind(sqlx::types::Json(&config.models)).bind(&config.proxy_id).bind(config.revision)
                .bind(config.dynamic_egress.as_ref().map(sqlx::types::Json))
                .bind(config.schedule.as_ref().map(sqlx::types::Json))
                .bind(match config.probe_profile { TurnStateProbeProfile::CodexCore => "codex_core", TurnStateProbeProfile::MinimalCompat => "minimal_compat" })
                .bind(config.adaptive_concurrency)
                .bind(i32::from(config.refresh_interval_minutes))
                .bind(i32::from(config.state_ttl_minutes))
                .execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
            if updated == 0 {
                let updated = sqlx::query("UPDATE turn_state_fetcher_configs SET enabled=$2,models=$3,proxy_id=$4,dynamic_egress=$6,schedule=$7,probe_profile=$8,adaptive_concurrency=$9,refresh_interval_minutes=$10,state_ttl_minutes=$11,revision=revision+1 WHERE account_id=$1 AND revision=$5")
                    .bind(&config.account_id).bind(config.enabled).bind(sqlx::types::Json(&config.models)).bind(&config.proxy_id).bind(config.revision)
                    .bind(config.dynamic_egress.as_ref().map(sqlx::types::Json))
                    .bind(config.schedule.as_ref().map(sqlx::types::Json))
                .bind(match config.probe_profile { TurnStateProbeProfile::CodexCore => "codex_core", TurnStateProbeProfile::MinimalCompat => "minimal_compat" })
                .bind(config.adaptive_concurrency)
                .bind(i32::from(config.refresh_interval_minutes))
                .bind(i32::from(config.state_ttl_minutes))
                    .execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
                if updated == 0 {
                    return Err(ProviderStoreError::new(
                        ProviderStoreErrorKind::Conflict,
                        "turn state config",
                    ));
                }
            }
            sqlx::query("DELETE FROM turn_state_fetch_attempts WHERE account_id=$1")
                .bind(&config.account_id)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            tx.commit().await.map_err(unavailable)
        })
    }

    fn values(&self) -> BoxFuture<'_, Result<Vec<TurnStateValue>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(sqlx::query_scalar::<_, sqlx::types::Json<TurnStateValue>>(
                "SELECT data FROM turn_state_values",
            )
            .fetch_all(&self.0)
            .await
            .map_err(unavailable)?
            .into_iter()
            .map(|v| v.0)
            .collect())
        })
    }

    fn save_values(
        &self,
        values: Vec<TurnStateValue>,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let mut tx = self.0.begin().await.map_err(unavailable)?;
            for value in values {
                sqlx::query("INSERT INTO turn_state_values(account_id,model,data) SELECT $1,$2,$3 WHERE EXISTS(SELECT 1 FROM provider_accounts WHERE id=$1 AND provider_kind='openai') ON CONFLICT(account_id,model) DO UPDATE SET data=EXCLUDED.data WHERE (turn_state_values.data->>'lastSeenAt')::bigint <= (EXCLUDED.data->>'lastSeenAt')::bigint")
                    .bind(&value.account_id).bind(&value.model).bind(sqlx::types::Json(&value)).execute(&mut *tx).await.map_err(unavailable)?;
            }
            tx.commit().await.map_err(unavailable)
        })
    }

    fn attempts(&self) -> BoxFuture<'_, Result<Vec<TurnStateFetchAttempt>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(
                sqlx::query_scalar::<_, sqlx::types::Json<TurnStateFetchAttempt>>(
                    "SELECT data FROM turn_state_fetch_attempts",
                )
                .fetch_all(&self.0)
                .await
                .map_err(unavailable)?
                .into_iter()
                .map(|v| v.0)
                .collect(),
            )
        })
    }

    fn save_attempt(
        &self,
        attempt: TurnStateFetchAttempt,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO turn_state_fetch_attempts(account_id,model,data) SELECT $1,$2,$3 FROM turn_state_fetcher_configs WHERE account_id=$1 AND revision=$4 AND models ? $2 ON CONFLICT(account_id,model) DO UPDATE SET data=EXCLUDED.data")
                .bind(&attempt.account_id).bind(&attempt.model).bind(sqlx::types::Json(&attempt)).bind(attempt.config_revision)
                .execute(&self.0).await.map_err(unavailable)?;
            Ok(())
        })
    }

    fn egress(
        &self,
        config: TurnStateFetcherConfig,
    ) -> BoxFuture<'_, Result<TurnStateFetchEgress, ProviderStoreError>> {
        Box::pin(async move {
            let row = sqlx::query("SELECT coalesce(a.concurrency_limit,r.max_concurrent_per_account) AS capacity,r.config_revision,r.request_interval_ms,p.proxy_url,p.last_test_success,c.proxy_id FROM turn_state_fetcher_configs c JOIN provider_accounts a ON a.id=c.account_id CROSS JOIN runtime_settings r LEFT JOIN outbound_proxies p ON p.id=c.proxy_id WHERE c.account_id=$1 AND c.revision=$2 AND c.enabled")
                .bind(&config.account_id).bind(config.revision).fetch_optional(&self.0).await.map_err(unavailable)?.ok_or_else(invalid)?;
            let proxy = if row
                .try_get::<Option<String>, _>("proxy_id")
                .map_err(unavailable)?
                .is_some()
            {
                if row
                    .try_get::<Option<bool>, _>("last_test_success")
                    .map_err(unavailable)?
                    != Some(true)
                {
                    return Err(invalid());
                }
                let url: String = row.try_get("proxy_url").map_err(unavailable)?;
                Some(gateway_core::account::OutboundProxy::parse(&url).map_err(|_| invalid())?)
            } else {
                None
            };
            Ok(TurnStateFetchEgress {
                config_revision: gateway_core::routing::ConfigRevision::new(
                    row.try_get::<i64, _>("config_revision")
                        .map_err(unavailable)?
                        .try_into()
                        .map_err(|_| invalid())?,
                )
                .map_err(|_| invalid())?,
                proxy,
                max_concurrent: row
                    .try_get::<i64, _>("capacity")
                    .map_err(unavailable)?
                    .try_into()
                    .map_err(|_| invalid())?,
                request_interval_ms: row
                    .try_get::<i64, _>("request_interval_ms")
                    .map_err(unavailable)?
                    .try_into()
                    .map_err(|_| invalid())?,
            })
        })
    }
}

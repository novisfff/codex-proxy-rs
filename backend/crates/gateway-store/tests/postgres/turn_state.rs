use super::{TestDatabase, provider_accounts::account};
use gateway_core::provider_ports::{ProviderStoreErrorKind, turn_state::*};
use gateway_store::postgres::{
    PgProviderAccountRepository, PgTurnStateStore, ProviderAccountRepository,
};

#[tokio::test]
async fn turn_state_schedule_roundtrip_update_clear_and_validation() {
    let Some(database) = TestDatabase::create("turn_state_schedule").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(account("acct_schedule", "user_schedule"))
        .await
        .unwrap();
    let store = PgTurnStateStore::new(database.pool.clone());
    let mut config = TurnStateFetcherConfig {
        account_id: "acct_schedule".to_owned(),
        enabled: true,
        models: vec!["gpt-test".to_owned()],
        proxy_id: None,
        dynamic_egress: None,
        schedule: Some(TurnStateFetcherSchedule {
            start_minute: 540,
            end_minute: 60,
        }),
        probe_profile: TurnStateProbeProfile::CodexCore,
        adaptive_concurrency: false,
        refresh_interval_minutes: 40,
        state_ttl_seconds: 3600,
        revision: 0,
    };
    store.save_config(config.clone()).await.unwrap();
    config.revision = 1;
    assert_eq!(store.configs().await.unwrap(), vec![config.clone()]);
    config.schedule = Some(TurnStateFetcherSchedule {
        start_minute: 0,
        end_minute: 0,
    });
    assert_eq!(
        store.save_config(config.clone()).await.unwrap_err().kind(),
        ProviderStoreErrorKind::InvalidData
    );
    config.schedule = None;
    config.probe_profile = TurnStateProbeProfile::MinimalCompat;
    config.adaptive_concurrency = true;
    config.refresh_interval_minutes = 12;
    config.state_ttl_seconds = 5401;
    store.save_config(config.clone()).await.unwrap();
    config.revision = 2;
    assert_eq!(store.configs().await.unwrap(), vec![config.clone()]);
    for (interval, ttl) in [(0, 3600), (61, 3600), (1, 0), (1, 86401)] {
        config.refresh_interval_minutes = interval;
        config.state_ttl_seconds = ttl;
        assert_eq!(
            store.save_config(config.clone()).await.unwrap_err().kind(),
            ProviderStoreErrorKind::InvalidData
        );
    }
    assert!(sqlx::query("UPDATE turn_state_fetcher_configs SET refresh_interval_minutes=91 WHERE account_id='acct_schedule'").execute(&database.pool).await.is_err());
    database.close().await;
}

#[tokio::test]
async fn turn_state_dynamic_egress_roundtrip_and_exclusive_static_proxy() {
    let Some(database) = TestDatabase::create("turn_state_dynamic").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(account("acct_dynamic", "user_dynamic"))
        .await
        .unwrap();
    let store = PgTurnStateStore::new(database.pool.clone());
    let mut config = TurnStateFetcherConfig {
        account_id: "acct_dynamic".to_owned(),
        enabled: true,
        models: vec!["gpt-test".to_owned()],
        proxy_id: None,
        dynamic_egress: Some(DynamicEgressSelection {
            instance: "azure".to_owned(),
            family: "ipv6".to_owned(),
        }),
        schedule: None,
        probe_profile: TurnStateProbeProfile::CodexCore,
        adaptive_concurrency: false,
        refresh_interval_minutes: 40,
        state_ttl_seconds: 3600,
        revision: 0,
    };
    store.save_config(config.clone()).await.unwrap();
    config.revision = 1;
    assert_eq!(store.configs().await.unwrap(), vec![config.clone()]);
    assert!(store.egress(config.clone()).await.unwrap().proxy.is_none());
    config.proxy_id = Some("other-proxy".to_owned());
    assert_eq!(
        store.save_config(config.clone()).await.unwrap_err().kind(),
        ProviderStoreErrorKind::InvalidData
    );
    config.proxy_id = None;
    config.dynamic_egress = None;
    store.save_config(config).await.unwrap();
    assert!(store.configs().await.unwrap()[0].dynamic_egress.is_none());
    database.close().await;
}

#[tokio::test]
async fn turn_state_persists_with_revision_fencing_and_account_cascade() {
    let Some(database) = TestDatabase::create("turn_state").await else {
        return;
    };
    let accounts = PgProviderAccountRepository::new(database.pool.clone());
    accounts
        .insert_provider_account(account("acct_ts", "user_ts"))
        .await
        .unwrap();
    let store = PgTurnStateStore::new(database.pool.clone());
    let mut config = TurnStateFetcherConfig {
        account_id: "acct_ts".to_owned(),
        enabled: true,
        models: vec!["gpt-5.4".to_owned()],
        proxy_id: None,
        dynamic_egress: None,
        schedule: None,
        probe_profile: TurnStateProbeProfile::CodexCore,
        adaptive_concurrency: false,
        refresh_interval_minutes: 40,
        state_ttl_seconds: 3600,
        revision: 0,
    };
    store.save_config(config.clone()).await.unwrap();
    assert_eq!(
        store.save_config(config.clone()).await.unwrap_err().kind(),
        ProviderStoreErrorKind::Conflict
    );
    config.revision = 1;
    let egress = store.egress(config.clone()).await.unwrap();
    assert!(egress.proxy.is_none());
    assert!(egress.max_concurrent > 0);
    let value = TurnStateValue {
        account_id: config.account_id.clone(),
        model: config.models[0].clone(),
        value: "a".repeat(292),
        acquired_at: 1000,
        last_seen_at: 2000,
        expires_at: 3601000,
        source: "fetcher".to_owned(),
        proxy_url: Some("socks5h://user:password@proxy.example:1111".to_owned()),
        session: Some(TurnStateSession {
            session_id: "probe-session".to_owned(),
            thread_id: "probe-thread".to_owned(),
            window_id: "probe-window".to_owned(),
            base_url: Some("https://chatgpt.com/backend-api".to_owned()),
        }),
    };
    store.save_values(vec![value.clone()]).await.unwrap();
    let mut older = value.clone();
    older.last_seen_at = 1000;
    older.value = "b".repeat(292);
    store.save_values(vec![older]).await.unwrap();
    assert_eq!(store.values().await.unwrap(), vec![value]);
    let attempt = TurnStateFetchAttempt {
        account_id: config.account_id.clone(),
        model: config.models[0].clone(),
        config_revision: 1,
        attempted_at: 1000,
        next_attempt_at: 61000,
        failures: 1,
        paused: false,
        status: "retrying".to_owned(),
        message: "retry".to_owned(),
        byte_length: Some(291),
        duration_ms: 1,
        input_tokens: None,
        output_tokens: None,
        exit_ip: None,
        search_concurrency: None,
    };
    store.save_attempt(attempt.clone()).await.unwrap();
    assert_eq!(store.attempts().await.unwrap().len(), 1);
    config.enabled = false;
    store.save_config(config).await.unwrap();
    store.save_attempt(attempt).await.unwrap();
    assert!(
        store.attempts().await.unwrap().is_empty(),
        "old revision cannot recreate cleared attempts"
    );
    sqlx::query("DELETE FROM provider_accounts WHERE id='acct_ts'")
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(store.values().await.unwrap().is_empty());
    assert!(store.configs().await.unwrap().is_empty());
    database.close().await;
}

#[tokio::test]
async fn turn_state_proxy_must_be_tested_and_cannot_be_deleted_while_bound() {
    use gateway_admin::{
        model::{
            MutationActor, MutationContext,
            proxies::{NewProxy, ProxyTestResult},
        },
        ports::proxy::ProxyStore,
    };
    use gateway_core::account::OutboundProxy;
    use gateway_store::postgres::PgProxyRepository;
    let Some(database) = TestDatabase::create("turn_state_proxy").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(account("acct_ts_proxy", "user_ts_proxy"))
        .await
        .unwrap();
    let proxies = PgProxyRepository::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "fetcher-proxy-test".to_owned(),
    };
    let saved = proxies
        .create(
            NewProxy {
                name: "Dedicated".to_owned(),
                proxy: OutboundProxy::parse("http://127.0.0.1:7890").unwrap(),
                location: None,
            },
            &context,
        )
        .await
        .unwrap()
        .record;
    let store = PgTurnStateStore::new(database.pool.clone());
    let mut config = TurnStateFetcherConfig {
        account_id: "acct_ts_proxy".to_owned(),
        enabled: true,
        models: vec!["gpt-5.4".to_owned()],
        proxy_id: Some(saved.id.clone()),
        dynamic_egress: None,
        schedule: None,
        probe_profile: TurnStateProbeProfile::CodexCore,
        adaptive_concurrency: false,
        refresh_interval_minutes: 40,
        state_ttl_seconds: 3600,
        revision: 0,
    };
    assert!(
        store.save_config(config.clone()).await.is_err(),
        "untested proxy must fail closed"
    );
    proxies
        .record_test(
            &saved.id,
            saved.revision,
            ProxyTestResult {
                success: true,
                latency_ms: 1,
                exit_ip: None,
                exit_ipv4: None,
                exit_ipv6: None,
                message: "test".to_owned(),
            },
            &context,
        )
        .await
        .unwrap();
    store.save_config(config.clone()).await.unwrap();
    config.revision = 1;
    assert!(store.egress(config).await.unwrap().proxy.is_some());
    assert!(
        proxies
            .delete(&saved.id, saved.revision, &context)
            .await
            .is_err(),
        "bound proxy must not become direct implicitly"
    );
    database.close().await;
}

#[tokio::test]
async fn turn_state_probe_history_is_bounded_and_removed_with_config() {
    let Some(database) = TestDatabase::create("turn_state_probes").await else {
        return;
    };
    PgProviderAccountRepository::new(database.pool.clone())
        .insert_provider_account(account("acct_probes", "user_probes"))
        .await
        .unwrap();
    let store = PgTurnStateStore::new(database.pool.clone());
    store
        .save_config(TurnStateFetcherConfig {
            account_id: "acct_probes".to_owned(),
            enabled: true,
            models: vec!["gpt-test".to_owned()],
            proxy_id: None,
            dynamic_egress: None,
            schedule: None,
            probe_profile: TurnStateProbeProfile::MinimalCompat,
            adaptive_concurrency: true,
            refresh_interval_minutes: 40,
            state_ttl_seconds: 3600,
            revision: 0,
        })
        .await
        .unwrap();
    for index in 0..205 {
        store
            .save_probe(TurnStateProbeRecord {
                id: format!("probe-{index}"),
                batch_id: "batch-test".to_owned(),
                account_id: "acct_probes".to_owned(),
                model: "gpt-test".to_owned(),
                config_revision: 1,
                profile: TurnStateProbeProfile::MinimalCompat,
                started_at: index,
                duration_ms: 10,
                outcome: "non_target".to_owned(),
                http_status: Some(200),
                http_version: Some("HTTP/2".to_owned()),
                byte_length: Some(312),
                repeated: false,
                endpoint: Some("/codex/responses".to_owned()),
                responses_lite: false,
                compressed: false,
                egress_instance: None,
                lease_id: None,
                exit_ip: None,
                fresh_connection: false,
            })
            .await
            .unwrap();
    }
    let probes = store.recent_probes().await.unwrap();
    assert_eq!(probes.len(), 200);
    assert_eq!(probes.first().unwrap().started_at, 204);
    assert_eq!(probes.last().unwrap().started_at, 5);
    assert_eq!(probes[0].byte_length, Some(312));
    sqlx::query("DELETE FROM turn_state_fetcher_configs WHERE account_id=$1")
        .bind("acct_probes")
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(store.recent_probes().await.unwrap().is_empty());
}

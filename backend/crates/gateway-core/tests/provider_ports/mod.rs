use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use gateway_core::account::{
    AccountRuntimeSignals, CredentialRevision, OpaqueProviderData, ProviderAccountId,
};
use gateway_core::provider_ports::{
    NewOAuthPendingFlow, OAuthPendingBinding, ProviderRefreshPolicy, ProviderSchedulingState,
    ProviderSessionAffinityKey, ProviderStoreErrorKind,
};
use gateway_core::routing::ProviderKind;

#[test]
fn turn_state_schedule_supports_beijing_time_boundaries_and_midnight() {
    use gateway_core::provider_ports::turn_state::TurnStateFetcherSchedule;
    let timestamp = |minute: i64| (minute - 480) * 60_000;
    let daytime = TurnStateFetcherSchedule {
        start_minute: 540,
        end_minute: 1020,
    };
    assert_eq!(daytime.remaining_ms(timestamp(539)), 0);
    assert_eq!(daytime.remaining_ms(timestamp(540)), 8 * 3_600_000);
    assert_eq!(daytime.remaining_ms(timestamp(1020) - 1), 1);
    assert_eq!(daytime.remaining_ms(timestamp(1020)), 0);
    let overnight = TurnStateFetcherSchedule {
        start_minute: 540,
        end_minute: 60,
    };
    assert_eq!(overnight.remaining_ms(timestamp(540)), 16 * 3_600_000);
    assert_eq!(overnight.remaining_ms(timestamp(0)), 3_600_000);
    assert_eq!(overnight.remaining_ms(timestamp(60)), 0);
    assert_eq!(overnight.remaining_ms(timestamp(539)), 0);
    assert_eq!(
        overnight.remaining_ms(timestamp(540) + 86_400_000),
        16 * 3_600_000
    );
    for (start_minute, end_minute) in [(0, 0), (1440, 60), (540, 1440)] {
        let invalid = TurnStateFetcherSchedule {
            start_minute,
            end_minute,
        };
        assert!(!invalid.is_valid());
        assert_eq!(invalid.remaining_ms(timestamp(600)), 0);
    }
}

#[test]
fn turn_state_schedule_defaults_to_all_day_for_existing_configs() {
    use gateway_core::provider_ports::turn_state::TurnStateFetcherConfig;
    let config: TurnStateFetcherConfig = serde_json::from_value(serde_json::json!({
        "accountId":"test", "enabled":true, "models":["test"], "proxyId":null, "revision":0
    }))
    .unwrap();
    assert!(config.schedule.is_none());
    assert!(config.allows_probe_at(0));
}

#[test]
fn oauth_pending_binding_debug_redacts_raw_value() {
    let binding = OAuthPendingBinding::try_new("must-not-appear").expect("valid binding");

    assert_eq!(format!("{binding:?}"), "OAuthPendingBinding([REDACTED])");
}

#[test]
fn provider_session_affinity_key_debug_is_opaque() {
    let key = ProviderSessionAffinityKey::try_new("opaque-session-key").expect("valid key");

    assert_eq!(format!("{key:?}"), "ProviderSessionAffinityKey([OPAQUE])");
}

#[test]
fn oauth_pending_ttl_rejects_zero_and_more_than_thirty_minutes() {
    let provider = ProviderKind::new("fixture").expect("valid provider");
    let flow = OAuthPendingBinding::try_new("flow").expect("valid flow");
    let owner = OAuthPendingBinding::try_new("owner").expect("valid owner");
    let payload = OpaqueProviderData::new(serde_json::Map::new());

    for ttl in [Duration::ZERO, Duration::from_secs(30 * 60 + 1)] {
        let error = NewOAuthPendingFlow::try_new(
            provider.clone(),
            flow.clone(),
            owner.clone(),
            ttl,
            payload.clone(),
        )
        .expect_err("invalid TTL must fail");
        assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData);
    }
}

#[test]
fn refresh_policy_requires_a_positive_margin() {
    let error = ProviderRefreshPolicy::try_new(
        Duration::ZERO,
        NonZeroU32::new(1).expect("positive concurrency"),
    )
    .expect_err("zero margin must fail");

    assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData);
}

#[test]
fn refresh_policy_should_mark_tokens_due_at_the_exact_configured_margin() {
    let policy = ProviderRefreshPolicy::try_new(
        Duration::from_secs(3_600),
        NonZeroU32::new(2).expect("positive concurrency"),
    )
    .expect("valid policy");
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
    let expires_at = observed_at + Duration::from_secs(7_200);

    assert!(!policy.is_refresh_due(expires_at, observed_at));
    assert!(policy.is_refresh_due(observed_at + Duration::from_secs(3_600), observed_at));
}

#[test]
fn refresh_policy_should_mark_expired_tokens_due() {
    let policy = ProviderRefreshPolicy::try_new(
        Duration::from_secs(3_600),
        NonZeroU32::new(1).expect("positive concurrency"),
    )
    .expect("valid policy");
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);

    assert!(policy.is_refresh_due(observed_at - Duration::from_secs(1), observed_at));
}

#[test]
fn scheduling_state_preserves_provider_neutral_signals() {
    let account = ProviderAccountId::new("acct_fixture").expect("valid account");
    let signals = BTreeMap::from([(
        account.clone(),
        AccountRuntimeSignals {
            in_flight: 2,
            last_started_at: None,
            quota_reset_at: None,
            quota_remaining_rank: Some(7),
            cooldown: None,
            failure_rate_basis_points: Some(125),
            first_output_latency_ms: Some(250),
        },
    )]);
    let state = ProviderSchedulingState::new(signals, 9);

    assert_eq!(state.signals()[&account].in_flight, 2);
    assert_eq!(
        state.signals()[&account].failure_rate_basis_points,
        Some(125)
    );
    assert_eq!(state.round_robin_cursor(), 9);
    assert_eq!(
        CredentialRevision::new(1).expect("positive revision").get(),
        1
    );
}

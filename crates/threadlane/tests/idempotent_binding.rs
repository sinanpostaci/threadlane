use std::time::Duration;

use threadlane::{
    ApiError, Error, ErrorCode, GetOrCreateBindingOutcome, GetOrCreateBindingRequest, Supervisor,
    SupervisorClient, SupervisorConfig,
};

const THREAD_A: &str = "11111111-1111-4111-8111-11111111111a";
const THREAD_B: &str = "22222222-2222-4222-8222-22222222222b";

#[tokio::test]
async fn supervisor_get_or_create_binding_creates_once_and_preserves_cursors_across_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let mut cfg = SupervisorConfig::new(&state_dir);
    cfg.tick_interval = Duration::from_millis(20);
    let handle = Supervisor::start(cfg.clone())
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");

    // 1. None -> create returns created=true.
    let created = client
        .get_or_create_binding(THREAD_A, "  native-operator-a  ")
        .await
        .expect("initial get_or_create_binding");
    assert!(created.created);
    assert!(created.binding.binding_id.starts_with("bnd_"));
    assert_eq!(created.binding.thread_uuid, THREAD_A);
    assert_eq!(created.binding.label, "native-operator-a");
    assert_eq!(created.binding.delivered_seq, 0);
    assert_eq!(created.binding.acked_seq, 0);

    // 2. Advance durable event cursors on the binding.
    let scoped = client
        .clone()
        .scoped(&created.binding.binding_id)
        .expect("scoped client");
    let run = scoped
        .start_demo(
            "demo-1".to_string(),
            "advance cursors".to_string(),
            Some(10),
        )
        .await
        .expect("start demo");
    let batch = scoped
        .pull_events(None, Some(10))
        .await
        .expect("pull events");
    assert!(!batch.events.is_empty());
    assert!(batch.delivered_seq >= 1);

    let ack = scoped
        .ack_events("ack-1".to_string(), batch.batch_id.clone())
        .await
        .expect("ack events");
    assert_eq!(ack.acked_seq, batch.to_seq);

    // Cancel the demo run so another event is emitted and delivered_seq > acked_seq after a pull.
    let _ = scoped
        .cancel(
            "cancel-1".to_string(),
            run.run_id,
            Some("done testing".to_string()),
        )
        .await
        .expect("cancel demo");
    let batch2 = scoped
        .pull_events(None, Some(10))
        .await
        .expect("pull second batch");
    assert!(batch2.delivered_seq > ack.acked_seq);

    // 3. Repeat get_or_create_binding with a different valid label returns created=false
    //    and preserves binding_id, original label, delivered_seq, and acked_seq.
    let repeated = client
        .get_or_create_binding(THREAD_A, "reconnect-label")
        .await
        .expect("repeat get_or_create_binding");
    assert!(!repeated.created);
    assert_eq!(repeated.binding.binding_id, created.binding.binding_id);
    assert_eq!(repeated.binding.thread_uuid, THREAD_A);
    assert_eq!(repeated.binding.label, "native-operator-a");
    assert_eq!(repeated.binding.delivered_seq, batch2.delivered_seq);
    assert_eq!(repeated.binding.acked_seq, ack.acked_seq);

    // 4. Verify raw HTTP status codes (201 Created vs 200 OK).
    let base_url = handle.descriptor().base_url().expect("base_url");
    let token = handle.descriptor().token.as_str().to_string();
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("raw http");

    let create_b_resp = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .bearer_auth(&token)
        .json(&GetOrCreateBindingRequest {
            thread_uuid: THREAD_B.to_string(),
            label: "thread-b".to_string(),
        })
        .send()
        .await
        .expect("POST create B");
    assert_eq!(create_b_resp.status(), reqwest::StatusCode::CREATED);
    let b_outcome: GetOrCreateBindingOutcome = create_b_resp.json().await.expect("decode B");
    assert!(b_outcome.created);

    let repeat_b_resp = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .bearer_auth(&token)
        .json(&GetOrCreateBindingRequest {
            thread_uuid: THREAD_B.to_string(),
            label: "thread-b-again".to_string(),
        })
        .send()
        .await
        .expect("POST repeat B");
    assert_eq!(repeat_b_resp.status(), reqwest::StatusCode::OK);
    let b_repeat_outcome: GetOrCreateBindingOutcome =
        repeat_b_resp.json().await.expect("decode repeat B");
    assert!(!b_repeat_outcome.created);
    assert_eq!(b_repeat_outcome.binding, b_outcome.binding);

    // 5. Restart the supervisor on the same state_dir and verify reconnect returns the exact same
    //    binding and cursors.
    handle.shutdown().await;
    let restarted = Supervisor::start(cfg).await.expect("restart supervisor");
    let after_restart = client
        .ensure_binding(THREAD_A, "post-restart")
        .await
        .expect("ensure_binding after restart");
    assert!(!after_restart.created);
    assert_eq!(after_restart.binding, repeated.binding);

    let all = client.list_bindings().await.expect("list bindings");
    assert_eq!(all, vec![repeated.binding, b_outcome.binding]);

    restarted.shutdown().await;
}

#[tokio::test]
async fn concurrent_supervisor_requests_create_single_binding() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let mut set = tokio::task::JoinSet::new();
    for idx in 0..12usize {
        let c = client.clone();
        set.spawn(async move {
            c.get_or_create_binding(THREAD_A, &format!("concurrent-{idx}"))
                .await
                .expect("concurrent get_or_create_binding")
        });
    }

    let mut outcomes = Vec::new();
    while let Some(res) = set.join_next().await {
        outcomes.push(res.expect("join task"));
    }

    let created_count = outcomes.iter().filter(|o| o.created).count();
    assert_eq!(
        created_count, 1,
        "expected 1 created=true, got {outcomes:?}"
    );

    let first_binding = &outcomes[0].binding;
    for outcome in &outcomes {
        assert_eq!(&outcome.binding, first_binding);
    }

    let listed = client.list_bindings().await.expect("list_bindings");
    assert_eq!(listed, vec![first_binding.clone()]);

    handle.shutdown().await;
}

#[tokio::test]
async fn multiple_pre_existing_bindings_return_conflict_without_mutations_and_preserve_create_binding(
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    // Existing create_binding API continues to create distinct bindings when invoked repeatedly.
    let b1 = client
        .create_binding(THREAD_A, "first-binding")
        .await
        .expect("create first");
    let b2 = client
        .create_binding(THREAD_A, "second-binding")
        .await
        .expect("create second");
    assert_ne!(b1.binding_id, b2.binding_id);

    // Another thread has a single clean binding and active cursor.
    let b_other = client
        .get_or_create_binding(THREAD_B, "isolated-thread-b")
        .await
        .expect("create B");
    assert!(b_other.created);
    let scoped_b = client
        .clone()
        .scoped(&b_other.binding.binding_id)
        .expect("scoped B");
    let run_b = scoped_b
        .start_demo(
            "demo-b".to_string(),
            "isolated demo".to_string(),
            Some(60_000),
        )
        .await
        .expect("start demo b");
    let batch_b = scoped_b.pull_events(None, Some(10)).await.expect("pull b");
    assert!(batch_b.delivered_seq >= 1);

    let before_list = client.list_bindings().await.expect("list before");

    // Calling get_or_create_binding on THREAD_A must fail with AmbiguousThreadBinding (HTTP 409).
    let err = client
        .get_or_create_binding(THREAD_A, "first-binding")
        .await
        .expect_err("duplicate bindings for THREAD_A must fail");
    let token_str = handle.descriptor().token.as_str();
    match err {
        Error::Remote(api_err) => {
            assert_eq!(api_err.code, ErrorCode::AmbiguousThreadBinding);
            assert!(!api_err.message.contains(token_str));
            let details = api_err.details.expect("ambiguity error details");
            assert_eq!(details["thread_uuid"], THREAD_A);
            assert_eq!(details["match_count"], 2);
            assert_eq!(
                details["binding_ids"],
                serde_json::json!([b1.binding_id, b2.binding_id])
            );
        }
        other => panic!("expected Error::Remote(AmbiguousThreadBinding), got {other:?}"),
    }

    // Verify raw HTTP status is 409 Conflict.
    let base_url = handle.descriptor().base_url().expect("base_url");
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("raw http");
    let raw_resp = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .bearer_auth(token_str)
        .json(&GetOrCreateBindingRequest {
            thread_uuid: THREAD_A.to_string(),
            label: "second-binding".to_string(),
        })
        .send()
        .await
        .expect("raw conflict request");
    assert_eq!(raw_resp.status(), reqwest::StatusCode::CONFLICT);

    // Verify no bindings or cursors were mutated on THREAD_A or THREAD_B.
    let after_list = client.list_bindings().await.expect("list after");
    assert_eq!(after_list, before_list);

    let b_repeat = client
        .get_or_create_binding(THREAD_B, "reconnect-b")
        .await
        .expect("THREAD_B unaffected");
    assert!(!b_repeat.created);
    assert_eq!(b_repeat.binding.binding_id, b_other.binding.binding_id);
    assert_eq!(b_repeat.binding.delivered_seq, batch_b.delivered_seq);

    let _ = scoped_b
        .cancel("cancel-b".to_string(), run_b.run_id, None)
        .await
        .expect("cancel demo b");
    handle.shutdown().await;
}

#[tokio::test]
async fn enforces_bearer_auth_input_validation_and_scoped_isolation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let base_url = handle.descriptor().base_url().expect("base_url");
    let token = handle.descriptor().token.as_str().to_string();
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("raw http");

    // 1. Missing Authorization header -> 401 Unauthorized.
    let no_auth = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .json(&GetOrCreateBindingRequest {
            thread_uuid: THREAD_A.to_string(),
            label: "unauth".to_string(),
        })
        .send()
        .await
        .expect("no auth send");
    assert_eq!(no_auth.status(), reqwest::StatusCode::UNAUTHORIZED);
    let no_auth_err: ApiError = no_auth.json().await.expect("no auth json");
    assert_eq!(no_auth_err.code, ErrorCode::Unauthorized);

    // 2. Wrong bearer token -> 401 Unauthorized.
    let bad_auth = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .bearer_auth("wrong-secret-token")
        .json(&GetOrCreateBindingRequest {
            thread_uuid: THREAD_A.to_string(),
            label: "bad-auth".to_string(),
        })
        .send()
        .await
        .expect("bad auth send");
    assert_eq!(bad_auth.status(), reqwest::StatusCode::UNAUTHORIZED);
    let bad_auth_err: ApiError = bad_auth.json().await.expect("bad auth json");
    assert_eq!(bad_auth_err.code, ErrorCode::Unauthorized);

    assert!(client.list_bindings().await.expect("empty").is_empty());

    // 3. Unknown JSON field -> 400 InvalidRequest.
    let unknown_field = raw_http
        .post(format!("{base_url}/v1/bindings/get-or-create"))
        .bearer_auth(&token)
        .body(
            serde_json::to_string(&serde_json::json!({
                "thread_uuid": THREAD_A,
                "label": "valid",
                "extra": true
            }))
            .expect("json"),
        )
        .send()
        .await
        .expect("unknown field send");
    assert_eq!(unknown_field.status(), reqwest::StatusCode::BAD_REQUEST);
    let unknown_err: ApiError = unknown_field.json().await.expect("unknown err");
    assert_eq!(unknown_err.code, ErrorCode::InvalidRequest);

    // 4. Create a valid binding for THREAD_A, then verify invalid UUID or label is rejected
    //    even when the binding already exists.
    let valid = client
        .get_or_create_binding(THREAD_A, "valid-label")
        .await
        .expect("create valid");
    assert!(valid.created);

    for (bad_uuid, bad_label, expected_field) in [
        ("not-a-uuid", "valid-label", "thread_uuid"),
        (
            "00000000-0000-0000-0000-000000000000",
            "valid-label",
            "thread_uuid",
        ),
        (
            "11111111-1111-4111-8111-11111111111A",
            "valid-label",
            "thread_uuid",
        ),
        (THREAD_A, "", "label"),
        (THREAD_A, "   ", "label"),
        (THREAD_A, "bad\nlabel", "label"),
    ] {
        let err = client
            .get_or_create_binding(bad_uuid, bad_label)
            .await
            .expect_err("invalid input must fail");
        match err {
            Error::Remote(api_err) => {
                assert_eq!(api_err.code, ErrorCode::ValidationFailed);
                let details = api_err.details.expect("validation details");
                assert_eq!(details["field"], expected_field);
            }
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
    }

    // 5. Scoped command endpoint must reject any attempt to invoke get_or_create_binding.
    let scoped_attempt = raw_http
        .post(format!(
            "{base_url}/v1/bindings/{}/command",
            valid.binding.binding_id
        ))
        .bearer_auth(&token)
        .body(
            serde_json::to_string(&serde_json::json!({
                "op": "get_or_create_binding",
                "thread_uuid": THREAD_B,
                "label": "smuggled"
            }))
            .expect("json"),
        )
        .send()
        .await
        .expect("scoped attempt");
    assert_eq!(scoped_attempt.status(), reqwest::StatusCode::BAD_REQUEST);
    let scoped_err: ApiError = scoped_attempt.json().await.expect("scoped err");
    assert_eq!(scoped_err.code, ErrorCode::InvalidRequest);

    assert_eq!(
        client.list_bindings().await.expect("list bindings"),
        vec![valid.binding]
    );

    handle.shutdown().await;
}

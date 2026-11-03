use serde_json::json;
use threadlane_native::coworker_workflow::{
    parse_and_verify, ExpectedTaskReviewIdentity, ReviewVerdictOutcome,
    StructuredTaskReviewModelResponse, MAX_TASK_REVIEW_RESPONSE_BYTES,
    TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
};
use threadlane_native::NativeErrorCode;

const TASK_ID: &str = "task_parser_review_01";
const CANDIDATE_ID: &str = "cand_parser_review_01";
const BASE_COMMIT: &str = "1111111111111111111111111111111111111111";
const HEAD_COMMIT: &str = "2222222222222222222222222222222222222222";
const TREE_SHA: &str = "3333333333333333333333333333333333333333";
const SUBJECT_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIFF_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const POLICY_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn expected_identity() -> ExpectedTaskReviewIdentity {
    ExpectedTaskReviewIdentity {
        task_id: TASK_ID.to_string(),
        attempt_no: 1,
        candidate_id: CANDIDATE_ID.to_string(),
        review_subject_digest: SUBJECT_DIGEST.to_string(),
        base_commit: BASE_COMMIT.to_string(),
        task_head_commit: HEAD_COMMIT.to_string(),
        task_tree_sha: TREE_SHA.to_string(),
        diff_digest: DIFF_DIGEST.to_string(),
        policy_digest: POLICY_DIGEST.to_string(),
    }
}

fn valid_accept_json() -> serde_json::Value {
    json!({
        "schema_version": TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "attempt_no": 1,
        "candidate_id": CANDIDATE_ID,
        "review_subject_digest": SUBJECT_DIGEST,
        "base_commit": BASE_COMMIT,
        "task_head_commit": HEAD_COMMIT,
        "task_tree_sha": TREE_SHA,
        "diff_digest": DIFF_DIGEST,
        "policy_digest": POLICY_DIGEST,
        "outcome": "accept",
        "findings": [],
        "required_in_scope_fixes": [],
        "conflicting_paths_or_interfaces": [],
        "summary": "Scoped changes satisfy contract and verification checks."
    })
}

#[test]
fn parses_valid_accept_correct_in_scope_and_scope_conflict_proposals() {
    let expected = expected_identity();
    let raw_accept = valid_accept_json().to_string();

    let verdict = parse_and_verify(&raw_accept, &expected).expect("valid accept must parse");
    let verdict_again = StructuredTaskReviewModelResponse::parse_and_verify(&raw_accept, &expected)
        .expect("inherent parse_and_verify must match");
    assert_eq!(verdict, verdict_again);
    assert!(verdict.verdict_id.starts_with("vrd_"));
    assert_eq!(verdict.verdict_id.len(), 36);
    assert_eq!(verdict.outcome, ReviewVerdictOutcome::Accept);
    assert!(!verdict.prose_may_advance_acceptance);
    assert!(verdict.allows_acceptance());

    let mut correct_in_scope = valid_accept_json();
    correct_in_scope["outcome"] = json!("correct_in_scope");
    correct_in_scope["findings"] = json!(["Missing edge-case check on empty input"]);
    correct_in_scope["required_in_scope_fixes"] =
        json!(["Reject empty raw reviewer payload before deserialization"]);
    let fix_verdict = parse_and_verify(&correct_in_scope.to_string(), &expected)
        .expect("valid correct_in_scope must parse");
    assert_eq!(fix_verdict.outcome, ReviewVerdictOutcome::CorrectInScope);
    assert!(!fix_verdict.allows_acceptance());
    assert_ne!(fix_verdict.verdict_id, verdict.verdict_id);

    let mut scope_conflict = valid_accept_json();
    scope_conflict["outcome"] = json!("scope_conflict");
    scope_conflict["findings"] = json!(["Requires modifying shared store signature"]);
    scope_conflict["conflicting_paths_or_interfaces"] =
        json!(["crates/threadlane-native/src/coworker_workflow/store.rs"]);
    let conflict_verdict = parse_and_verify(&scope_conflict.to_string(), &expected)
        .expect("valid scope_conflict must parse");
    assert_eq!(
        conflict_verdict.outcome,
        ReviewVerdictOutcome::ScopeConflict
    );
    assert!(!conflict_verdict.allows_acceptance());
}

#[test]
fn rejects_empty_truncated_oversized_fenced_and_wrong_schema_responses() {
    let expected = expected_identity();
    let raw_accept = valid_accept_json().to_string();

    let mut wrong_schema = valid_accept_json();
    wrong_schema["schema_version"] = json!(2);
    let wrong_schema_str = wrong_schema.to_string();
    let fenced = format!("```json\n{raw_accept}\n```");
    let prose_wrapped = format!("Approved! {raw_accept}");
    let truncated = &raw_accept[..raw_accept.len() - 4];
    let oversized = format!(
        "{{\"pad\":\"{}\"}}",
        "x".repeat(MAX_TASK_REVIEW_RESPONSE_BYTES + 1)
    );

    for invalid in [
        "",
        "   \n\t  ",
        "Looks good to me, ship it.",
        truncated,
        fenced.as_str(),
        prose_wrapped.as_str(),
        oversized.as_str(),
        wrong_schema_str.as_str(),
    ] {
        let err = parse_and_verify(invalid, &expected).expect_err("must reject invalid payload");
        assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    }
}

#[test]
fn rejects_any_mismatched_expected_identity_field() {
    let expected = expected_identity();
    let mismatches = [
        ("task_id", json!("task_other_99")),
        ("attempt_no", json!(2)),
        ("candidate_id", json!("cand_other_99")),
        (
            "review_subject_digest",
            json!("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
        ),
        (
            "base_commit",
            json!("4444444444444444444444444444444444444444"),
        ),
        (
            "task_head_commit",
            json!("5555555555555555555555555555555555555555"),
        ),
        (
            "task_tree_sha",
            json!("6666666666666666666666666666666666666666"),
        ),
        (
            "diff_digest",
            json!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        ),
        (
            "policy_digest",
            json!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        ),
    ];

    for (field, wrong_value) in mismatches {
        let mut payload = valid_accept_json();
        payload[field] = wrong_value;
        let err = parse_and_verify(&payload.to_string(), &expected)
            .expect_err("mismatched identity field must be rejected");
        assert_eq!(err.code, NativeErrorCode::ValidationFailed, "field {field}");
    }
}

#[test]
fn rejects_unknown_fields_contradictory_accept_and_positive_prose_escalation() {
    let expected = expected_identity();

    let mut forge_prose_flag = valid_accept_json();
    forge_prose_flag["prose_may_advance_acceptance"] = json!(true);
    assert_eq!(
        parse_and_verify(&forge_prose_flag.to_string(), &expected)
            .expect_err("model cannot set prose_may_advance_acceptance")
            .code,
        NativeErrorCode::ValidationFailed
    );

    let mut forge_verdict_id = valid_accept_json();
    forge_verdict_id["verdict_id"] = json!("vrd_00000000000000000000000000000000");
    assert_eq!(
        parse_and_verify(&forge_verdict_id.to_string(), &expected)
            .expect_err("model cannot supply verdict_id")
            .code,
        NativeErrorCode::ValidationFailed
    );

    let mut contradictory_accept = valid_accept_json();
    contradictory_accept["required_in_scope_fixes"] = json!(["Fix edge case before accepting"]);
    assert_eq!(
        parse_and_verify(&contradictory_accept.to_string(), &expected)
            .expect_err("accept with required fixes must fail")
            .code,
        NativeErrorCode::ValidationFailed
    );

    let mut blocked_with_positive_prose = valid_accept_json();
    blocked_with_positive_prose["outcome"] = json!("blocked");
    blocked_with_positive_prose["findings"] = json!(["Integration check did not run"]);
    blocked_with_positive_prose["summary"] =
        json!("LGTM! Everything looks great and should be accepted immediately.");
    let blocked = parse_and_verify(&blocked_with_positive_prose.to_string(), &expected)
        .expect("valid blocked verdict with positive summary must parse");
    assert!(!blocked.prose_may_advance_acceptance);
    assert!(!blocked.allows_acceptance());
}

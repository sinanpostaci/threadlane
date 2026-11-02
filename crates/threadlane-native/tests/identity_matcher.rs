use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::os::unix::fs::symlink;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use threadlane_native::{
    encode_connection_identity_tool_result, match_native_thread_identity,
    match_native_thread_identity_with_budget, validate_canonical_project_cwd,
    verify_thread_hint_candidate, ConnectionEpochState, ConnectionIdentity, IdentityMatchBudget,
    IdentityMatchErrorKind, NativeError, NativeErrorCode, NativeIdentityMatcher,
    NativeIdentityProbeClient, NativeLoadedThreadPage, NativeMcpServerStatus,
    NativeMcpToolCallResult, NativeThread, NativeVerificationStatus, CONNECTION_IDENTITY_TOOL,
    MCP_SERVER_NAME,
};

const THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
const THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";
const THREAD_C: &str = "0196ed00-0000-7000-8000-00000000000c";
const THREAD_D: &str = "0196ed00-0000-7000-8000-00000000000d";

const NONCE_A: &str = "nce_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const EPOCH_A: &str = "epc_1111111111111111111111111111111111111111111111111111111111111111";
const NONCE_B: &str = "nce_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const EPOCH_B: &str = "epc_2222222222222222222222222222222222222222222222222222222222222222";
const NONCE_C: &str = "nce_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const EPOCH_C: &str = "epc_3333333333333333333333333333333333333333333333333333333333333333";

fn make_identity(nonce: &str, epoch: &str, pid: u32) -> ConnectionIdentity {
    ConnectionIdentity {
        connection_nonce: nonce.to_string(),
        epoch_id: epoch.to_string(),
        server_pid: pid,
        state: ConnectionEpochState::Unbound,
        enrolled_binding_id: None,
        verification_gate: NativeVerificationStatus::NeedsVerification,
    }
}

fn make_thread(id: &str, cwd: &str, status: &str, project_id: Option<&str>) -> NativeThread {
    NativeThread {
        id: id.to_string(),
        project_id: project_id.map(str::to_string),
        title: Some(format!("Confidential Title {id}")),
        cwd: cwd.to_string(),
        status: status.to_string(),
        model: Some("gpt-6-astra".to_string()),
        effort: Some("high".to_string()),
        can_accept_direct_input: Some(true),
    }
}

fn running_mcp_status() -> Vec<NativeMcpServerStatus> {
    vec![NativeMcpServerStatus {
        name: MCP_SERVER_NAME.to_string(),
        auth_status: "unsupported".to_string(),
        tool_names: vec![CONNECTION_IDENTITY_TOOL.to_string(), "status".to_string()],
        tools_error: None,
        runtime_status: Some("connected".to_string()),
    }]
}

fn stopped_mcp_status(runtime_status: &str) -> Vec<NativeMcpServerStatus> {
    vec![NativeMcpServerStatus {
        name: MCP_SERVER_NAME.to_string(),
        auth_status: "unsupported".to_string(),
        tool_names: Vec::new(),
        tools_error: None,
        runtime_status: Some(runtime_status.to_string()),
    }]
}

#[derive(Default)]
struct FakeProbeState {
    loaded_pages: VecDeque<Result<NativeLoadedThreadPage, NativeError>>,
    default_loaded_page: Option<NativeLoadedThreadPage>,
    thread_reads: BTreeMap<String, VecDeque<Result<NativeThread, NativeError>>>,
    mcp_statuses: BTreeMap<String, VecDeque<Result<Vec<NativeMcpServerStatus>, NativeError>>>,
    identity_calls: BTreeMap<String, VecDeque<Result<NativeMcpToolCallResult, NativeError>>>,
    loaded_list_cursors: Vec<Option<String>>,
    read_calls: Vec<String>,
    status_calls: Vec<String>,
    tool_calls: Vec<String>,
    delay: Option<Duration>,
}

#[derive(Default)]
struct FakeProbeClient {
    state: Mutex<FakeProbeState>,
}

impl FakeProbeClient {
    fn push_loaded_page(&self, page: Result<NativeLoadedThreadPage, NativeError>) {
        self.state.lock().unwrap().loaded_pages.push_back(page);
    }

    fn set_default_loaded_ids(&self, ids: Vec<&str>) {
        self.state.lock().unwrap().default_loaded_page = Some(NativeLoadedThreadPage {
            thread_ids: ids.into_iter().map(str::to_string).collect(),
            next_cursor: None,
        });
    }

    fn set_candidate(
        &self,
        thread: NativeThread,
        statuses: Vec<NativeMcpServerStatus>,
        identity: Option<ConnectionIdentity>,
    ) {
        let mut st = self.state.lock().unwrap();
        let id = thread.id.clone();
        st.thread_reads
            .entry(id.clone())
            .or_default()
            .push_back(Ok(thread));
        st.mcp_statuses
            .entry(id.clone())
            .or_default()
            .push_back(Ok(statuses));
        if let Some(ident) = identity {
            st.identity_calls
                .entry(id)
                .or_default()
                .push_back(Ok(encode_connection_identity_tool_result(&ident)));
        }
    }

    fn push_thread_read(&self, id: &str, res: Result<NativeThread, NativeError>) {
        self.state
            .lock()
            .unwrap()
            .thread_reads
            .entry(id.to_string())
            .or_default()
            .push_back(res);
    }

    fn push_identity_call(&self, id: &str, res: Result<NativeMcpToolCallResult, NativeError>) {
        self.state
            .lock()
            .unwrap()
            .identity_calls
            .entry(id.to_string())
            .or_default()
            .push_back(res);
    }
}

fn pop_or_clone<T: Clone>(
    queue: &mut VecDeque<Result<T, NativeError>>,
    label: &str,
) -> Result<T, NativeError> {
    if queue.len() > 1 {
        queue.pop_front().unwrap()
    } else if let Some(front) = queue.front() {
        front.clone()
    } else {
        Err(NativeError::new(
            NativeErrorCode::ThreadNotFound,
            format!("no fake response configured for {label}"),
        ))
    }
}

#[async_trait]
impl NativeIdentityProbeClient for FakeProbeClient {
    async fn list_loaded_threads(
        &self,
        cursor: Option<String>,
    ) -> Result<NativeLoadedThreadPage, NativeError> {
        let delay = self.state.lock().unwrap().delay;
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let mut st = self.state.lock().unwrap();
        st.loaded_list_cursors.push(cursor);
        if let Some(res) = st.loaded_pages.pop_front() {
            return res;
        }
        st.default_loaded_page.clone().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::TransportUnavailable,
                "no loaded page configured",
            )
        })
    }

    async fn read_thread(&self, thread_id: &str) -> Result<NativeThread, NativeError> {
        let mut st = self.state.lock().unwrap();
        st.read_calls.push(thread_id.to_string());
        let queue = st.thread_reads.entry(thread_id.to_string()).or_default();
        pop_or_clone(queue, "read_thread")
    }

    async fn list_mcp_server_status(
        &self,
        thread_id: &str,
        _server_name: &str,
    ) -> Result<Vec<NativeMcpServerStatus>, NativeError> {
        let mut st = self.state.lock().unwrap();
        st.status_calls.push(thread_id.to_string());
        let queue = st.mcp_statuses.entry(thread_id.to_string()).or_default();
        pop_or_clone(queue, "list_mcp_server_status")
    }

    async fn call_connection_identity(
        &self,
        thread_id: &str,
        _server_name: &str,
    ) -> Result<NativeMcpToolCallResult, NativeError> {
        let mut st = self.state.lock().unwrap();
        st.tool_calls.push(thread_id.to_string());
        let queue = st.identity_calls.entry(thread_id.to_string()).or_default();
        pop_or_clone(queue, "call_connection_identity")
    }
}

#[tokio::test]
async fn hint_is_checked_against_only_its_own_connection_and_remains_unverified() {
    let tmp = TempDir::new().unwrap();
    let cwd = fs::canonicalize(tmp.path()).unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 4101);
    let client = FakeProbeClient::default();
    client.set_candidate(
        make_thread(THREAD_A, cwd.to_str().unwrap(), "active", Some("proj-1")),
        running_mcp_status(),
        Some(expected.clone()),
    );
    let candidate = verify_thread_hint_candidate(&client, THREAD_A, &expected, &cwd)
        .await
        .expect("matching hint and connection should be a candidate");
    assert_eq!(candidate.thread_id, THREAD_A);
    assert!(!candidate.native_per_call_identity_verified);
    assert_eq!(
        candidate.verification_gate,
        NativeVerificationStatus::NeedsVerification
    );
    let state = client.state.lock().unwrap();
    assert!(state.loaded_list_cursors.is_empty());
    assert_eq!(state.tool_calls, vec![THREAD_A, THREAD_A]);
}

#[tokio::test]
async fn forged_or_unreachable_hint_never_establishes_a_candidate() {
    let tmp = TempDir::new().unwrap();
    let cwd = fs::canonicalize(tmp.path()).unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 4101);
    let wrong = FakeProbeClient::default();
    wrong.set_candidate(
        make_thread(THREAD_B, cwd.to_str().unwrap(), "active", None),
        running_mcp_status(),
        Some(make_identity(NONCE_B, EPOCH_B, 4102)),
    );
    let err = verify_thread_hint_candidate(&wrong, THREAD_B, &expected, &cwd)
        .await
        .expect_err("another thread's connection cannot validate the hint");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IdentityMismatch);

    let denied = FakeProbeClient::default();
    denied.set_candidate(
        make_thread(THREAD_A, cwd.to_str().unwrap(), "active", None),
        running_mcp_status(),
        None,
    );
    denied.push_identity_call(
        THREAD_A,
        Err(NativeError::rpc(-32600, "direct input denied")),
    );
    let err = verify_thread_hint_candidate(&denied, THREAD_A, &expected, &cwd)
        .await
        .expect_err("an inaccessible hinted thread must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::UnprobeableCandidate);
}

#[tokio::test]
async fn unique_match_scans_all_loaded_projects_dedupes_ids_skips_stopped_and_rechecks_census() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let other_cwd = "/tmp/other-project-workspace";

    let client = FakeProbeClient::default();
    // Initial 2-page loaded census (with duplicate THREAD_B across pages) + 2-page recheck census.
    for _ in 0..2 {
        client.push_loaded_page(Ok(NativeLoadedThreadPage {
            thread_ids: vec![THREAD_A.to_string(), THREAD_B.to_string()],
            next_cursor: Some("page-2".to_string()),
        }));
        client.push_loaded_page(Ok(NativeLoadedThreadPage {
            thread_ids: vec![
                THREAD_B.to_string(),
                THREAD_C.to_string(),
                THREAD_D.to_string(),
            ],
            next_cursor: None,
        }));
    }

    let id_a = make_identity(NONCE_A, EPOCH_A, 4101);
    let id_b = make_identity(NONCE_B, EPOCH_B, 4102);
    let id_d = make_identity(NONCE_C, EPOCH_C, 4104);

    // THREAD_A: target project, active, distinct identity.
    client.set_candidate(
        make_thread(THREAD_A, cwd_str, "active", Some("proj-1")),
        running_mcp_status(),
        Some(id_a),
    );
    // THREAD_B: target project, idle, matches expected_identity.
    client.set_candidate(
        make_thread(THREAD_B, cwd_str, "idle", Some("proj-1")),
        running_mcp_status(),
        Some(id_b.clone()),
    );
    // THREAD_C: target project, idle, MCP server notStarted -> must NOT call connection_identity.
    client.set_candidate(
        make_thread(THREAD_C, cwd_str, "idle", Some("proj-1")),
        stopped_mcp_status("notStarted"),
        None,
    );
    // THREAD_D: DIFFERENT project cwd, active, distinct MCP process -> must be probed to prove no cross-project sharing.
    client.set_candidate(
        make_thread(THREAD_D, other_cwd, "active", Some("proj-2")),
        running_mcp_status(),
        Some(id_d),
    );

    let matched = match_native_thread_identity(&client, &id_b, &project_cwd)
        .await
        .expect("unique match should succeed");

    assert_eq!(matched.thread_id, THREAD_B);
    assert_eq!(matched.cwd, project_cwd);
    assert_eq!(matched.project_id.as_deref(), Some("proj-1"));
    assert_eq!(matched.thread_status, "idle");
    assert_eq!(matched.server_pid, 4102);
    assert_eq!(matched.epoch_state, ConnectionEpochState::Unbound);
    assert_eq!(
        matched.verification_gate,
        NativeVerificationStatus::NeedsVerification
    );
    assert!(!matched.native_per_call_identity_verified);
    assert_eq!(matched.pages_scanned, 2);
    assert_eq!(matched.candidates_examined, 4);
    assert_eq!(matched.candidates_probed, 3);
    assert_eq!(matched.candidates_skipped_mcp_not_running, 1);

    let st = client.state.lock().unwrap();
    // 2 pages on initial census + 2 pages on post-discovery recheck.
    assert_eq!(
        st.loaded_list_cursors,
        vec![
            None,
            Some("page-2".to_string()),
            None,
            Some("page-2".to_string())
        ]
    );
    // All 4 unique loaded threads read during scan + 1 post-discovery re-read of THREAD_B.
    assert_eq!(
        st.read_calls,
        vec![THREAD_A, THREAD_B, THREAD_C, THREAD_D, THREAD_B]
    );
    assert_eq!(
        st.status_calls,
        vec![THREAD_A, THREAD_B, THREAD_C, THREAD_D]
    );
    // THREAD_C was skipped; THREAD_A, THREAD_B, THREAD_D probed + 1 post-discovery re-probe of THREAD_B.
    assert_eq!(st.tool_calls, vec![THREAD_A, THREAD_B, THREAD_D, THREAD_B]);

    let dbg = format!("{matched:?}");
    assert!(!dbg.contains(NONCE_B));
    assert!(!dbg.contains(EPOCH_B));
    assert!(!dbg.contains(cwd_str));
    assert!(dbg.contains("<redacted-canonical-cwd>"));
}

#[tokio::test]
async fn cross_project_shared_connection_and_cross_project_cwd_mismatch_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let other_cwd = "/tmp/other-project-root";

    // 1. Cross-project shared connection: THREAD_A in project_cwd and THREAD_B in other_cwd share id_shared.
    let shared_client = FakeProbeClient::default();
    shared_client.set_default_loaded_ids(vec![THREAD_A, THREAD_B]);
    let id_shared = make_identity(NONCE_A, EPOCH_A, 5001);
    shared_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "active", Some("proj-a")),
        running_mcp_status(),
        Some(id_shared.clone()),
    );
    shared_client.set_candidate(
        make_thread(THREAD_B, other_cwd, "idle", Some("proj-b")),
        running_mcp_status(),
        Some(id_shared.clone()),
    );

    let shared_err = match_native_thread_identity(&shared_client, &id_shared, &project_cwd)
        .await
        .expect_err("shared connection across projects must fail closed");
    assert_eq!(
        shared_err.kind(),
        IdentityMatchErrorKind::AmbiguousSharedConnection
    );
    assert_eq!(
        shared_err.error_code(),
        NativeErrorCode::SharedConnectionRejected
    );

    // 2. Unrelated duplicate PID between two other loaded threads also fails closed.
    let dup_pid_client = FakeProbeClient::default();
    dup_pid_client.set_default_loaded_ids(vec![THREAD_A, THREAD_B, THREAD_C]);
    let expected = make_identity(NONCE_A, EPOCH_A, 5010);
    dup_pid_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "active", None),
        running_mcp_status(),
        Some(expected.clone()),
    );
    dup_pid_client.set_candidate(
        make_thread(THREAD_B, cwd_str, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_B, EPOCH_B, 5099)),
    );
    dup_pid_client.set_candidate(
        make_thread(THREAD_C, other_cwd, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_C, EPOCH_C, 5099)),
    );
    let dup_pid_err = match_native_thread_identity(&dup_pid_client, &expected, &project_cwd)
        .await
        .expect_err("duplicate server_pid across any loaded threads must fail closed");
    assert_eq!(
        dup_pid_err.kind(),
        IdentityMatchErrorKind::AmbiguousSharedConnection
    );

    // 3. Unique match belongs to another project's cwd -> fails closed with ThreadMetadataMismatch.
    let wrong_cwd_client = FakeProbeClient::default();
    wrong_cwd_client.set_default_loaded_ids(vec![THREAD_A]);
    wrong_cwd_client.set_candidate(
        make_thread(THREAD_A, other_cwd, "active", Some("proj-other")),
        running_mcp_status(),
        Some(expected.clone()),
    );
    let cwd_err = match_native_thread_identity(&wrong_cwd_client, &expected, &project_cwd)
        .await
        .expect_err("unique match in another project cwd must fail closed");
    assert_eq!(
        cwd_err.kind(),
        IdentityMatchErrorKind::ThreadMetadataMismatch
    );
    assert_eq!(cwd_err.error_code(), NativeErrorCode::ProjectMismatch);
}

#[tokio::test]
async fn active_v2_subagent_disallowing_direct_tool_call_fails_closed_as_unprobeable() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();

    let client = FakeProbeClient::default();
    client.set_default_loaded_ids(vec![THREAD_A, THREAD_B]);

    let expected = make_identity(NONCE_A, EPOCH_A, 6100);
    // THREAD_A is the parent chat that matches expected_identity.
    client.set_candidate(
        make_thread(THREAD_A, cwd_str, "active", Some("proj-1")),
        running_mcp_status(),
        Some(expected.clone()),
    );
    // THREAD_B is an active multi-agent v2 subagent: thread/read and mcpServerStatus/list succeed,
    // but mcpServer/tool/call(connection_identity) fails with JSON-RPC -32600.
    client.set_candidate(
        make_thread(THREAD_B, cwd_str, "active", Some("proj-1")),
        running_mcp_status(),
        None,
    );
    client.push_identity_call(
        THREAD_B,
        Err(NativeError::rpc(
            -32600,
            "direct app-server input is not allowed for multi-agent v2 sub-agents",
        )),
    );

    let err = match_native_thread_identity(&client, &expected, &project_cwd)
        .await
        .expect_err("inaccessible v2 subagent must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::UnprobeableCandidate);
    assert_eq!(err.error_code(), NativeErrorCode::UnsupportedCapability);
    assert_eq!(err.rpc_code, Some(-32600));
}

#[tokio::test]
async fn changing_loaded_census_or_unloaded_candidate_fails_closed() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 7100);

    // 1. Second `thread/loaded/list` census adds a new loaded thread (THREAD_B).
    let added_client = FakeProbeClient::default();
    added_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: vec![THREAD_A.to_string()],
        next_cursor: None,
    }));
    added_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: vec![THREAD_A.to_string(), THREAD_B.to_string()],
        next_cursor: None,
    }));
    added_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(expected.clone()),
    );

    let add_err = match_native_thread_identity(&added_client, &expected, &project_cwd)
        .await
        .expect_err("concurrent thread addition in loaded census must fail closed");
    assert_eq!(add_err.kind(), IdentityMatchErrorKind::ChangingCensus);
    assert_eq!(add_err.error_code(), NativeErrorCode::ExpiredEpoch);

    // 2. A thread returned in `thread/loaded/list` reports `not_loaded` during `thread/read`.
    let unloaded_client = FakeProbeClient::default();
    unloaded_client.set_default_loaded_ids(vec![THREAD_A, THREAD_B]);
    unloaded_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(expected.clone()),
    );
    unloaded_client.set_candidate(
        make_thread(THREAD_B, cwd_str, "not_loaded", None),
        stopped_mcp_status("notStarted"),
        None,
    );

    let unload_err = match_native_thread_identity(&unloaded_client, &expected, &project_cwd)
        .await
        .expect_err("candidate reporting not_loaded after thread/loaded/list must fail closed");
    assert_eq!(unload_err.kind(), IdentityMatchErrorKind::ChangingCensus);
}

#[tokio::test]
async fn pagination_exhaustion_and_cursor_anomalies_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 8100);

    // 1. max_pages exhausted while next_cursor is still present.
    let page_cap_client = FakeProbeClient::default();
    page_cap_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: vec![THREAD_A.to_string()],
        next_cursor: Some("cursor-2".to_string()),
    }));
    let matcher = NativeIdentityMatcher::with_budget(IdentityMatchBudget {
        max_pages: 1,
        max_candidates: 10,
        max_probed_candidates: 10,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let err = matcher
        .match_candidate(&page_cap_client, &expected, &project_cwd)
        .await
        .expect_err("incomplete pagination must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IncompletePagination);
    assert_eq!(err.error_code(), NativeErrorCode::OutputOverflow);

    // 2. max_candidates exceeded.
    let cand_cap_client = FakeProbeClient::default();
    cand_cap_client.set_default_loaded_ids(vec![THREAD_A, THREAD_B]);
    let err = match_native_thread_identity_with_budget(
        &cand_cap_client,
        &expected,
        &project_cwd,
        &IdentityMatchBudget {
            max_pages: 4,
            max_candidates: 1,
            max_probed_candidates: 1,
            timeout: Duration::from_secs(5),
        },
    )
    .await
    .expect_err("exceeding max_candidates must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IncompletePagination);

    // 3. max_probed_candidates exceeded.
    let probe_cap_client = FakeProbeClient::default();
    probe_cap_client.set_default_loaded_ids(vec![THREAD_A, THREAD_B]);
    probe_cap_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_B, EPOCH_B, 8101)),
    );
    probe_cap_client.set_candidate(
        make_thread(THREAD_B, cwd_str, "idle", None),
        running_mcp_status(),
        Some(expected.clone()),
    );
    let err = match_native_thread_identity_with_budget(
        &probe_cap_client,
        &expected,
        &project_cwd,
        &IdentityMatchBudget {
            max_pages: 4,
            max_candidates: 4,
            max_probed_candidates: 1,
            timeout: Duration::from_secs(5),
        },
    )
    .await
    .expect_err("exceeding max_probed_candidates must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IncompletePagination);

    // 4. Empty page with continuation cursor.
    let empty_cur_client = FakeProbeClient::default();
    empty_cur_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: Vec::new(),
        next_cursor: Some("cursor-next".to_string()),
    }));
    let err = match_native_thread_identity(&empty_cur_client, &expected, &project_cwd)
        .await
        .expect_err("empty page with cursor must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IncompletePagination);

    // 5. Cursor cycle.
    let cycle_client = FakeProbeClient::default();
    cycle_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: vec![THREAD_A.to_string()],
        next_cursor: Some("cursor-dup".to_string()),
    }));
    cycle_client.push_loaded_page(Ok(NativeLoadedThreadPage {
        thread_ids: vec![THREAD_B.to_string()],
        next_cursor: Some("cursor-dup".to_string()),
    }));
    let err = match_native_thread_identity(&cycle_client, &expected, &project_cwd)
        .await
        .expect_err("cursor cycle must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IncompletePagination);
}

#[tokio::test]
async fn wrong_epoch_or_pid_stale_recheck_and_empty_or_no_match_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 9100);

    // 1. Empty `thread/loaded/list` -> EmptyProject.
    let empty_client = FakeProbeClient::default();
    empty_client.set_default_loaded_ids(Vec::new());
    let err = match_native_thread_identity(&empty_client, &expected, &project_cwd)
        .await
        .expect_err("empty loaded census must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::EmptyProject);
    assert_eq!(err.error_code(), NativeErrorCode::ThreadNotFound);

    // 2. Unrelated loaded thread -> NoMatchingThread.
    let no_match_client = FakeProbeClient::default();
    no_match_client.set_default_loaded_ids(vec![THREAD_A]);
    no_match_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_B, EPOCH_B, 9101)),
    );
    let err = match_native_thread_identity(&no_match_client, &expected, &project_cwd)
        .await
        .expect_err("no matching thread must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::NoMatchingThread);

    // 3. Same nonce but rotated epoch_id -> StaleResult.
    let stale_epoch_client = FakeProbeClient::default();
    stale_epoch_client.set_default_loaded_ids(vec![THREAD_A]);
    stale_epoch_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_A, EPOCH_B, 9100)),
    );
    let err = match_native_thread_identity(&stale_epoch_client, &expected, &project_cwd)
        .await
        .expect_err("rotated epoch must fail as StaleResult");
    assert_eq!(err.kind(), IdentityMatchErrorKind::StaleResult);
    assert_eq!(err.error_code(), NativeErrorCode::ExpiredEpoch);

    // 4. Same nonce + epoch but wrong PID -> IdentityMismatch.
    let wrong_pid_client = FakeProbeClient::default();
    wrong_pid_client.set_default_loaded_ids(vec![THREAD_A]);
    wrong_pid_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(make_identity(NONCE_A, EPOCH_A, 9999)),
    );
    let err = match_native_thread_identity(&wrong_pid_client, &expected, &project_cwd)
        .await
        .expect_err("wrong server_pid must fail as IdentityMismatch");
    assert_eq!(err.kind(), IdentityMatchErrorKind::IdentityMismatch);
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 5. Post-discovery re-read sees rotated epoch_id -> StaleResult.
    let recheck_stale_client = FakeProbeClient::default();
    recheck_stale_client.set_default_loaded_ids(vec![THREAD_A]);
    recheck_stale_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        running_mcp_status(),
        Some(expected.clone()),
    );
    recheck_stale_client.push_identity_call(
        THREAD_A,
        Ok(encode_connection_identity_tool_result(&make_identity(
            NONCE_A, EPOCH_B, 9100,
        ))),
    );
    let err = match_native_thread_identity(&recheck_stale_client, &expected, &project_cwd)
        .await
        .expect_err("epoch change on post-discovery recheck must fail as StaleResult");
    assert_eq!(err.kind(), IdentityMatchErrorKind::StaleResult);

    // 6. Post-discovery re-read sees thread unloaded (`not_loaded`) -> StaleResult.
    let recheck_unloaded_client = FakeProbeClient::default();
    recheck_unloaded_client.set_default_loaded_ids(vec![THREAD_A]);
    recheck_unloaded_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "active", None),
        running_mcp_status(),
        Some(expected.clone()),
    );
    recheck_unloaded_client.push_thread_read(
        THREAD_A,
        Ok(make_thread(THREAD_A, cwd_str, "not_loaded", None)),
    );
    let err = match_native_thread_identity(&recheck_unloaded_client, &expected, &project_cwd)
        .await
        .expect_err(
            "thread becoming not_loaded on post-discovery recheck must fail as StaleResult",
        );
    assert_eq!(err.kind(), IdentityMatchErrorKind::StaleResult);
}

#[tokio::test]
async fn absent_server_unsupported_status_metadata_mismatch_and_provider_errors_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let project_cwd = fs::canonicalize(tmp.path()).unwrap();
    let cwd_str = project_cwd.to_str().unwrap();
    let expected = make_identity(NONCE_A, EPOCH_A, 9500);

    // 1. Symlink project_cwd rejected as SecurityViolation.
    let link_dir = tmp.path().join("symlink-cwd");
    let target_dir = tmp.path().join("real-cwd");
    fs::create_dir_all(&target_dir).unwrap();
    symlink(&target_dir, &link_dir).unwrap();
    let symlink_err = validate_canonical_project_cwd(&link_dir).expect_err("symlink cwd rejected");
    assert_eq!(
        symlink_err.kind(),
        IdentityMatchErrorKind::SecurityViolation
    );

    // 2. Absent threadlane MCP server entry -> AbsentOrInvalidMcpServer.
    let absent_client = FakeProbeClient::default();
    absent_client.set_default_loaded_ids(vec![THREAD_A]);
    absent_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        Vec::new(),
        None,
    );
    let err = match_native_thread_identity(&absent_client, &expected, &project_cwd)
        .await
        .expect_err("missing threadlane MCP entry must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::AbsentOrInvalidMcpServer);
    assert_eq!(err.error_code(), NativeErrorCode::NotConfigured);

    // 3. Unsupported thread status (`system_error`) and MCP status (`starting`) -> UnsupportedStatus.
    let bad_status_client = FakeProbeClient::default();
    bad_status_client.set_default_loaded_ids(vec![THREAD_A]);
    bad_status_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "system_error", None),
        running_mcp_status(),
        None,
    );
    let err = match_native_thread_identity(&bad_status_client, &expected, &project_cwd)
        .await
        .expect_err("system_error status must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::UnsupportedStatus);

    // A missing runtime status must not be interpreted as a connected server.
    let missing_status_client = FakeProbeClient::default();
    missing_status_client.set_default_loaded_ids(vec![THREAD_A]);
    let mut missing_status = running_mcp_status();
    missing_status[0].runtime_status = None;
    missing_status_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        missing_status,
        None,
    );
    let err = match_native_thread_identity(&missing_status_client, &expected, &project_cwd)
        .await
        .expect_err("missing MCP runtime status must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::UnsupportedStatus);

    let starting_mcp_client = FakeProbeClient::default();
    starting_mcp_client.set_default_loaded_ids(vec![THREAD_A]);
    starting_mcp_client.set_candidate(
        make_thread(THREAD_A, cwd_str, "idle", None),
        stopped_mcp_status("starting"),
        None,
    );
    let err = match_native_thread_identity(&starting_mcp_client, &expected, &project_cwd)
        .await
        .expect_err("starting MCP runtime status must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::UnsupportedStatus);

    // 4. Mismatched thread.id returned by `thread/read` -> ThreadMetadataMismatch.
    let id_mismatch_client = FakeProbeClient::default();
    id_mismatch_client.set_default_loaded_ids(vec![THREAD_A]);
    id_mismatch_client.push_thread_read(THREAD_A, Ok(make_thread(THREAD_B, cwd_str, "idle", None)));
    let err = match_native_thread_identity(&id_mismatch_client, &expected, &project_cwd)
        .await
        .expect_err("mismatched thread.id on read_thread must fail");
    assert_eq!(err.kind(), IdentityMatchErrorKind::ThreadMetadataMismatch);

    // 5. Provider error is not swallowed as no-match and never echoes provider text.
    let provider_err_client = FakeProbeClient::default();
    provider_err_client.set_default_loaded_ids(vec![THREAD_A]);
    provider_err_client.push_thread_read(
        THREAD_A,
        Err(NativeError::new(
            NativeErrorCode::TransportUnavailable,
            format!("socket /tmp/secret.sock failed for {NONCE_A} and {EPOCH_A}"),
        )),
    );
    let err = match_native_thread_identity(&provider_err_client, &expected, &project_cwd)
        .await
        .expect_err("transport error must fail closed");
    assert_eq!(err.kind(), IdentityMatchErrorKind::ProviderError);
    assert!(!err.message().contains(NONCE_A));
    assert!(!err.message().contains(EPOCH_A));
    assert!(!err.message().contains("/tmp/secret.sock"));
    assert!(!err.message().contains("socket"));
    assert!(err.message().contains("transport_unavailable"));

    // 6. Timeout budget exceeded -> TimedOut.
    let slow_client = FakeProbeClient::default();
    slow_client.set_default_loaded_ids(vec![THREAD_A]);
    slow_client.state.lock().unwrap().delay = Some(Duration::from_millis(80));
    let err = match_native_thread_identity_with_budget(
        &slow_client,
        &expected,
        &project_cwd,
        &IdentityMatchBudget {
            max_pages: 4,
            max_candidates: 10,
            max_probed_candidates: 10,
            timeout: Duration::from_millis(15),
        },
    )
    .await
    .expect_err("slow client must time out");
    assert_eq!(err.kind(), IdentityMatchErrorKind::TimedOut);
    assert_eq!(err.error_code(), NativeErrorCode::TimedOut);
}

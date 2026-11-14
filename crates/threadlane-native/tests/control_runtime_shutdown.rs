//! Focused end-to-end regressions for verified incompatible control-runtime refusals that race
//! an independent shutdown.
//!
//! These tests pin the early-refusal rule: once authenticated `control_runtime_info` returns a
//! valid `ApprovedHost` role or a protocol newer than the caller, that refusal is final even if
//! the next readiness read retires, closes, or times out while the owner's lock disappears.
//!
//! Each case uses an isolated `0700` temporary directory, a loopback fake owner that holds the
//! real `supervisor.lock` and writes a real descriptor, and a canonical executable spy that only
//! counts invocations. No external service, provider, user store, or ambient runtime is touched.

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tempfile::TempDir;
use threadlane::descriptor::{
    remove_owned_descriptor, write_descriptor_atomic, RuntimeDescriptor, SecretToken, StateLock,
    DESCRIPTOR_FILENAME,
};
use threadlane::protocol::{
    ControlRuntimeCapabilities, ControlRuntimeInfoResponse, ControlRuntimeRole,
    ControlRuntimeRuntimeState, CONTROL_RUNTIME_PROTOCOL_VERSION, SUPERVISOR_RETIRING_MESSAGE,
};
use threadlane::{ApiError, ErrorCode as ThreadlaneErrorCode};
use threadlane_native::{ensure_control_runtime, ControlRuntimeErrorCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long the fake owner keeps a possible next-bindings request available before it cleans up
/// independently. The corrected helper refuses immediately after the info response and never
/// sends that request; the old helper reaches it well inside this window.
const BINDINGS_GRACE: Duration = Duration::from_secs(1);

/// Bound for the fake owner's total serving lifetime, including the info response.
const OWNER_LIFETIME: Duration = Duration::from_secs(10);

/// Bound for every fake-owner HTTP header read.
const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Bound for awaiting the fake owner's descriptor/lock cleanup.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound for the full `ensure_control_runtime` call under test.
const ENSURE_TIMEOUT: Duration = Duration::from_secs(5);

/// A validated authenticated advertisement that local policy must refuse before readiness.
#[derive(Clone, Copy)]
struct IncompatibleSpec {
    role: ControlRuntimeRole,
    protocol_version: u32,
    package_version: &'static str,
}

impl IncompatibleSpec {
    fn approved_host() -> Self {
        Self {
            role: ControlRuntimeRole::ApprovedHost,
            protocol_version: CONTROL_RUNTIME_PROTOCOL_VERSION,
            package_version: "approved-host-shutdown-fixture",
        }
    }

    fn newer_protocol() -> Self {
        Self {
            role: ControlRuntimeRole::Baseline,
            protocol_version: CONTROL_RUNTIME_PROTOCOL_VERSION + 1,
            package_version: "newer-protocol-shutdown-fixture",
        }
    }
}

/// What the fake owner makes available to the old helper's bindings readiness request.
#[derive(Clone, Copy)]
enum BindingShutdown {
    /// Answer the bindings request with the authenticated supervisor-retiring error.
    Retiring,
    /// Remove ownership metadata and then close the bindings transport without a response.
    TransportClose,
}

struct ShutdownFixture {
    _tmp: TempDir,
    state_dir: PathBuf,
    count_file: PathBuf,
    executable: PathBuf,
}

impl ShutdownFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("create isolated TempDir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize TempDir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 TempDir root");

        let state_dir = root.join("control-state");
        fs::create_dir(&state_dir).expect("create state_dir");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 state_dir");

        let bin_dir = root.join("bin");
        fs::create_dir(&bin_dir).expect("create bin_dir");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 bin_dir");

        let count_file = root.join("successor-invocations.txt");
        let executable_path = bin_dir.join("threadlane");
        write_executable_spy(&executable_path, &count_file);
        let executable = fs::canonicalize(&executable_path).expect("canonicalize spy executable");

        Self {
            _tmp: tmp,
            state_dir,
            count_file,
            executable,
        }
    }
}

fn write_executable_spy(path: &Path, count_file: &Path) {
    // A single small append keeps each invocation count atomic, and the spy exits immediately
    // without launching any external service or provider.
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf 'launch\\n' >> \"{}\"\nexit 0\n",
        count_file.display()
    );
    let mut file = File::create(path).expect("create executable spy");
    file.write_all(script.as_bytes())
        .expect("write executable spy");
    file.sync_all().expect("sync executable spy");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 executable spy");
}

fn read_invocation_count(path: &Path) -> usize {
    match fs::read_to_string(path) {
        Ok(contents) => contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        Err(_) => 0,
    }
}

/// Owns the fake runtime's descriptor and lock until the bounded shutdown schedule completes.
/// Removing the descriptor while the lock is still held, then releasing the lock, mirrors a real
/// owner's clean shutdown ordering.
struct OwnedRuntimeCleanup {
    state_dir: PathBuf,
    descriptor: RuntimeDescriptor,
    _lock: StateLock,
}

impl Drop for OwnedRuntimeCleanup {
    fn drop(&mut self) {
        remove_owned_descriptor(&self.state_dir, &self.descriptor);
    }
}

struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
}

struct FakeOwnerTask {
    listener: TcpListener,
    info: ControlRuntimeInfoResponse,
    token: SecretToken,
    binding_shutdown: BindingShutdown,
    retire_requests: Arc<AtomicUsize>,
    binding_requests: Arc<AtomicUsize>,
}

impl FakeOwnerTask {
    async fn run(self, state_dir: &Path, descriptor: &RuntimeDescriptor) {
        let overall_deadline = tokio::time::Instant::now() + OWNER_LIFETIME;
        let mut cleanup_deadline: Option<tokio::time::Instant> = None;
        loop {
            let deadline = match cleanup_deadline {
                Some(cleanup_deadline) => cleanup_deadline.min(overall_deadline),
                None => overall_deadline,
            };
            let accepted = match tokio::time::timeout_at(deadline, self.listener.accept()).await {
                Ok(accepted) => accepted,
                Err(_) => return,
            };
            let Ok((mut socket, _)) = accepted else {
                return;
            };
            let Some(request) = read_http_request(&mut socket).await else {
                continue;
            };

            let expected_auth = format!("Bearer {}", self.token.as_str());
            if request.authorization.as_deref() != Some(expected_auth.as_str()) {
                write_json(
                    &mut socket,
                    "401 Unauthorized",
                    &ApiError::new(ThreadlaneErrorCode::Unauthorized, "invalid bearer token"),
                )
                .await;
                continue;
            }

            match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/v1/control-runtime/info") => {
                    write_json(&mut socket, "200 OK", &self.info).await;
                    cleanup_deadline =
                        Some((tokio::time::Instant::now() + BINDINGS_GRACE).min(overall_deadline));
                }
                ("GET", "/v1/bindings") => {
                    self.binding_requests.fetch_add(1, Ordering::SeqCst);
                    match self.binding_shutdown {
                        BindingShutdown::Retiring => {
                            write_json(
                                &mut socket,
                                "503 Service Unavailable",
                                &ApiError::new(
                                    ThreadlaneErrorCode::InternalError,
                                    SUPERVISOR_RETIRING_MESSAGE,
                                ),
                            )
                            .await;
                        }
                        BindingShutdown::TransportClose => {
                            // Remove ownership metadata before closing the transport so the old
                            // helper's post-failure ownership check observes a released runtime
                            // and proceeds to the unauthorized replacement path.
                            remove_owned_descriptor(state_dir, descriptor);
                            let _ = socket.shutdown().await;
                        }
                    }
                    return;
                }
                ("POST", "/v1/control-runtime/retire") => {
                    self.retire_requests.fetch_add(1, Ordering::SeqCst);
                    write_json(
                        &mut socket,
                        "503 Service Unavailable",
                        &ApiError::new(
                            ThreadlaneErrorCode::InternalError,
                            SUPERVISOR_RETIRING_MESSAGE,
                        ),
                    )
                    .await;
                }
                _ => {
                    write_json(
                        &mut socket,
                        "404 Not Found",
                        &ApiError::new(ThreadlaneErrorCode::InvalidRequest, "endpoint not found"),
                    )
                    .await;
                }
            }
        }
    }
}

struct FakeOwner {
    info: ControlRuntimeInfoResponse,
    descriptor: RuntimeDescriptor,
    retire_requests: Arc<AtomicUsize>,
    binding_requests: Arc<AtomicUsize>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FakeOwner {
    async fn start(
        state_dir: &Path,
        spec: IncompatibleSpec,
        binding_shutdown: BindingShutdown,
    ) -> Self {
        let lock = StateLock::acquire(state_dir).expect("fake owner acquires supervisor.lock");
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind fake owner loopback listener");
        let port = listener
            .local_addr()
            .expect("fake owner listener local_addr")
            .port();
        let descriptor = RuntimeDescriptor {
            version: 1,
            host: "127.0.0.1".to_string(),
            port,
            pid: std::process::id(),
            token: SecretToken::generate(),
        };
        write_descriptor_atomic(state_dir, &descriptor).expect("write fake owner descriptor");

        let info = ControlRuntimeInfoResponse {
            protocol_version: spec.protocol_version,
            package_version: spec.package_version.to_string(),
            role: spec.role,
            state: ControlRuntimeRuntimeState::Ready,
            instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            pid: std::process::id(),
            notifications_configured: false,
            workers_configured: false,
            capabilities: ControlRuntimeCapabilities {
                bindings: true,
                scoped_backlog: true,
                event_pull_ack: true,
                guarded_worker_backlog: true,
                idle_retire: true,
                approved_host_control: spec.role == ControlRuntimeRole::ApprovedHost,
            },
        };

        let retire_requests = Arc::new(AtomicUsize::new(0));
        let binding_requests = Arc::new(AtomicUsize::new(0));
        let task = FakeOwnerTask {
            listener,
            info: info.clone(),
            token: descriptor.token.clone(),
            binding_shutdown,
            retire_requests: Arc::clone(&retire_requests),
            binding_requests: Arc::clone(&binding_requests),
        };
        let task_state_dir = state_dir.to_path_buf();
        let task_descriptor = descriptor.clone();
        let handle = tokio::spawn(async move {
            let _owner = OwnedRuntimeCleanup {
                state_dir: task_state_dir.clone(),
                descriptor: task_descriptor.clone(),
                _lock: lock,
            };
            task.run(&task_state_dir, &task_descriptor).await;
        });

        Self {
            info,
            descriptor,
            retire_requests,
            binding_requests,
            task: Some(handle),
        }
    }

    async fn await_cleanup(mut self) -> OwnerCleanupEvidence {
        let mut task = self.task.take().expect("fake owner task present");
        match tokio::time::timeout(CLEANUP_TIMEOUT, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("fake owner task failed: {err}"),
            Err(_) => {
                task.abort();
                panic!("fake owner did not finish bounded cleanup within {CLEANUP_TIMEOUT:?}");
            }
        }
        OwnerCleanupEvidence {
            retire_requests: self.retire_requests.load(Ordering::SeqCst),
            binding_requests: self.binding_requests.load(Ordering::SeqCst),
        }
    }
}

impl Drop for FakeOwner {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct OwnerCleanupEvidence {
    retire_requests: usize,
    binding_requests: usize,
}

async fn read_http_request(socket: &mut TcpStream) -> Option<HttpRequest> {
    let mut buffer = Vec::new();
    let header_read =
        tokio::time::timeout(HTTP_READ_TIMEOUT, read_http_header(socket, &mut buffer)).await;
    let header_end = match header_read {
        Ok(Some(header_end)) => header_end,
        _ => return None,
    };

    let header_text = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut authorization = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("authorization") {
            authorization = Some(value.trim().to_string());
        }
    }

    Some(HttpRequest {
        method,
        path,
        authorization,
    })
}

async fn read_http_header(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<usize> {
    let mut chunk = [0u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            return Some(pos + 4);
        }
        if buffer.len() > 16 * 1024 {
            return None;
        }
    }
}

async fn write_json<T: Serialize>(socket: &mut TcpStream, status: &str, value: &T) {
    let body = serde_json::to_vec(value).expect("serialize fake owner response");
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(header.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}

async fn assert_verified_incompatible_refusal(
    spec: IncompatibleSpec,
    binding_shutdown: BindingShutdown,
) {
    let fixture = ShutdownFixture::new();
    let owner = FakeOwner::start(&fixture.state_dir, spec, binding_shutdown).await;
    let expected_info = owner.info.clone();
    let expected_port = owner.descriptor.port;

    let err = ensure_control_runtime(&fixture.state_dir, &fixture.executable, ENSURE_TIMEOUT)
        .await
        .expect_err("a verified incompatible runtime must refuse ensure");

    let evidence = owner.await_cleanup().await;
    let successor_invocations = read_invocation_count(&fixture.count_file);
    assert_eq!(
        err.error_code(),
        ControlRuntimeErrorCode::IncompatibleRuntime,
        "unexpected error: {err}; binding_requests={}, retire_requests={}, successor_invocations={successor_invocations}",
        evidence.binding_requests,
        evidence.retire_requests
    );
    assert_eq!(
        evidence.binding_requests, 0,
        "the verified refusal must happen before any bindings readiness request"
    );
    assert_eq!(
        evidence.retire_requests, 0,
        "a refused runtime must never receive a retire request"
    );
    assert_eq!(
        successor_invocations, 0,
        "a refused runtime must never cause a successor invocation"
    );

    let message = err.message();
    assert!(
        message.contains(&format!("pid {}", expected_info.pid)),
        "refusal lost the verified pid: {message}"
    );
    assert!(
        message.contains(&format!("port {expected_port}")),
        "refusal lost the verified port: {message}"
    );
    assert!(
        message.contains(&format!(
            "reports protocol {}",
            expected_info.protocol_version
        )),
        "refusal lost the verified protocol: {message}"
    );
    assert!(
        message.contains(&format!("package `{}`", expected_info.package_version)),
        "refusal lost the verified package version: {message}"
    );
    assert!(
        message.contains(&format!("role `{:?}`", expected_info.role)),
        "refusal lost the verified role: {message}"
    );
    match spec.role {
        ControlRuntimeRole::ApprovedHost => assert!(
            message.contains("not baseline"),
            "approved-host refusal must stay explicit: {message}"
        ),
        _ => assert!(
            message.contains("newer than this caller's supported version"),
            "newer-protocol refusal must stay explicit: {message}"
        ),
    }

    assert!(
        !fixture.state_dir.join(DESCRIPTOR_FILENAME).exists(),
        "fake owner must cleanly remove its own descriptor"
    );
    let lock = StateLock::acquire(&fixture.state_dir)
        .expect("fake owner must release its own supervisor.lock");
    drop(lock);
}

#[tokio::test]
async fn approved_host_with_retiring_bindings_refuses_before_shutdown() {
    assert_verified_incompatible_refusal(
        IncompatibleSpec::approved_host(),
        BindingShutdown::Retiring,
    )
    .await;
}

#[tokio::test]
async fn approved_host_with_transport_close_refuses_before_shutdown() {
    assert_verified_incompatible_refusal(
        IncompatibleSpec::approved_host(),
        BindingShutdown::TransportClose,
    )
    .await;
}

#[tokio::test]
async fn newer_protocol_with_retiring_bindings_refuses_before_shutdown() {
    assert_verified_incompatible_refusal(
        IncompatibleSpec::newer_protocol(),
        BindingShutdown::Retiring,
    )
    .await;
}

#[tokio::test]
async fn newer_protocol_with_transport_close_refuses_before_shutdown() {
    assert_verified_incompatible_refusal(
        IncompatibleSpec::newer_protocol(),
        BindingShutdown::TransportClose,
    )
    .await;
}

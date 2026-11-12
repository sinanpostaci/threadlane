//! Synthetic startup-deadline and caller-cancellation regressions.
//!
//! This module is a child of `approved_tests` so it can reuse the private fixture helpers
//! without exposing them in production code. Every external effect below is owned by the
//! temporary fixture; no installed provider executable is ever invoked.

use std::fs::File;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde_json::json;

use crate::fs_safety::acquire_cross_process_lock;
use crate::host_launcher::{execute_start_approved_task_with_launcher, ApprovedHostLauncher};

use super::*;

/// An authority-store read required before launching must respect the approved-host startup
/// deadline instead of blocking the request for the duration of an unrelated `authority.lock`
/// holder. After the request has returned, release the holder and prove that it cannot cause a
/// late host spawn.
#[tokio::test]
async fn initial_authority_lock_is_bounded_by_startup_deadline_and_never_spawns_late() {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-authority-lock-deadline",
        &fixture.binding_a,
        default_plan_tasks("Synthetic authority lock deadline regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-authority-lock-deadline",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_000_000,
    );

    let spy_exe = fixture.root.join("bin/spy-authority-lock-host");
    let spy_count = fixture.root.join("spy-authority-lock-count.txt");
    let spy_argv = fixture.root.join("spy-authority-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-authority-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic authority-lock host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create authority-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach authority-lock test launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    // Hold the real authority cross-process lock from a plain OS thread. The lock acquisition
    // is intentionally outside the Tokio runtime so setup cannot block the async test worker.
    let authority_lock_path = fixture.authority.lock_path().to_path_buf();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel::<()>();
    let authority_holder = std::thread::spawn(move || {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&authority_lock_path)
            .expect("open authority.lock for synthetic holder");
        file.lock_exclusive()
            .expect("acquire exclusive authority.lock");
        let _ = locked_tx.send(());
        std::thread::sleep(Duration::from_millis(800));
        // Dropping the File releases the fs2 lock after the fixed synthetic hold.
        drop(file);
    });
    tokio::time::timeout(Duration::from_millis(500), locked_rx)
        .await
        .expect("synthetic authority.lock holder must signal acquisition")
        .expect("synthetic authority.lock holder must acquire before timing out");

    let started = Instant::now();
    let request_result = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await;
    let elapsed = started.elapsed();

    // Join the holder before asserting. The elapsed value above is captured before this join so
    // a broken implementation cannot hide its unbounded authority-lock wait behind cleanup.
    tokio::task::spawn_blocking(move || {
        authority_holder
            .join()
            .expect("synthetic authority.lock holder must not panic");
    })
    .await
    .expect("authority.lock holder join task must complete");

    // Give any incorrectly detached startup task time to wake after the lock is released.
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(
        request_result.is_err(),
        "start_approved_task must fail closed while authority.lock blocks the initial read"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "initial authority-lock wait must respect the 250ms startup deadline (elapsed: {elapsed:?})"
    );
    assert_eq!(
        read_invocation_count(&spy_count),
        0,
        "a timed-out authority-lock wait must never spawn threadlane-host, before or after release"
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        0,
        "no approved worker task may be dispatched without a started host"
    );
}

/// The final enrollment recheck immediately before a fresh-host spawn must be bounded by the
/// startup deadline. This drives the actual launcher entry point with an owned closure that
/// contends for a real `pairing.lock` on its third invocation, then releases it from a separate
/// thread. A corrected off-thread recheck must time out without spawning.
#[tokio::test]
async fn final_enrollment_recheck_lock_is_bounded_by_startup_deadline_and_never_spawns() {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-final-enrollment-lock-deadline",
        &fixture.binding_a,
        default_plan_tasks("Synthetic final enrollment lock deadline regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-final-enrollment-lock-deadline",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_100_000,
    );

    let spy_exe = fixture.root.join("bin/spy-enrollment-lock-host");
    let spy_count = fixture.root.join("spy-enrollment-lock-count.txt");
    let spy_argv = fixture.root.join("spy-enrollment-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-enrollment-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic enrollment-lock host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create enrollment-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy.clone())
        .expect("attach enrollment-lock test launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let recheck_calls = Arc::new(AtomicUsize::new(0));
    let holder_slot = Arc::new(Mutex::new(None::<JoinHandle<()>>));

    // Everything captured here is owned and 'static. In particular, the fixture is never
    // borrowed, so a corrected launcher can run this recheck on a blocking worker safely.
    let recheck_pairing_store = fixture.pairing_store.clone();
    let recheck_pairing_dir = fixture.pairing_dir.clone();
    let recheck_nonce = mcp.connection_nonce().to_string();
    let recheck_epoch = mcp.epoch_id().to_string();
    let recheck_binding = fixture.binding_a.clone();
    let recheck_calls_for_closure = Arc::clone(&recheck_calls);
    let holder_slot_for_closure = Arc::clone(&holder_slot);

    let recheck = move || {
        let attempt = recheck_calls_for_closure.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 3 {
            let guard = acquire_cross_process_lock(
                &recheck_pairing_dir,
                "pairing.lock",
                "synthetic final enrollment recheck",
            )
            .map_err(|err| {
                crate::approved_mcp::format_approved_scoped_error(
                    &recheck_binding,
                    "start_approved_task",
                    err.code(),
                    "Synthetic final enrollment recheck could not acquire the pairing lock.",
                )
            })?;

            // The closure itself must then block in the real enrollment verification. The lock
            // holder releases after a fixed 700ms so the test can never leave pairing.lock held.
            let holder = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(700));
                drop(guard);
            });
            let mut slot = holder_slot_for_closure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = Some(holder);
        }

        recheck_pairing_store
            .verify_enrolled_binding(&recheck_nonce, &recheck_epoch)
            .map(|_| ())
            .map_err(|err| {
                crate::approved_mcp::format_approved_scoped_error(
                    &recheck_binding,
                    "start_approved_task",
                    err.code(),
                    "Synthetic final enrollment recheck failed.",
                )
            })
    };

    let started = Instant::now();
    let request_result = execute_start_approved_task_with_launcher(
        Arc::clone(&fixture.authority),
        fixture.sessions_dir.clone(),
        fixture.binding_a.clone(),
        THREAD_UUID_A.to_string(),
        StartApprovedTaskParams {
            approval_id: approval.approval_id.clone(),
            task_id: "task-root-agy".to_string(),
        },
        Some(Arc::new(ApprovedHostLauncher::new(policy))),
        recheck,
    )
    .await;
    let elapsed = started.elapsed();

    let holder = holder_slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
        .expect("third enrollment recheck must acquire the synthetic pairing.lock");
    tokio::task::spawn_blocking(move || {
        holder
            .join()
            .expect("synthetic pairing.lock holder must not panic");
    })
    .await
    .expect("pairing.lock holder join task must complete");

    // Release happened above; this grace window catches any detached late spawn attempt.
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(
        recheck_calls.load(Ordering::SeqCst) >= 3,
        "test must reach the final enrollment recheck before asserting bounded behavior"
    );
    assert!(
        request_result.is_err(),
        "start_approved_task must fail closed when the final enrollment lock outlives the deadline"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "final enrollment-lock wait must respect the 250ms startup deadline (elapsed: {elapsed:?})"
    );
    assert_eq!(
        read_invocation_count(&spy_count),
        0,
        "a timed-out final enrollment recheck must never spawn threadlane-host after lock release"
    );
}

/// Caller cancellation while the final enrollment recheck contends for `pairing.lock` must not
/// become a detached spawn. The policy deadline is deliberately 2s; the third recheck acquires the
/// real lock and starts a finite 700ms holder before signaling the actor, which then drops the owned
/// request through a 100ms timeout.
#[tokio::test]
async fn caller_cancellation_during_final_enrollment_lock_never_spawns_host() {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-canceled-final-enrollment-lock",
        &fixture.binding_a,
        default_plan_tasks("Synthetic canceled final enrollment lock regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-canceled-final-enrollment-lock",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_200_000,
    );

    let spy_exe = fixture.root.join("bin/spy-canceled-enrollment-lock-host");
    let spy_count = fixture.root.join("spy-canceled-enrollment-lock-count.txt");
    let spy_argv = fixture.root.join("spy-canceled-enrollment-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-canceled-enrollment-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic canceled enrollment-lock host policy")
        .with_startup_timeout(Duration::from_secs(2))
        .expect("2s startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create canceled enrollment-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy.clone())
        .expect("attach canceled enrollment-lock test launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let recheck_calls = Arc::new(AtomicUsize::new(0));
    let holder_slot = Arc::new(Mutex::new(None::<JoinHandle<()>>));
    let (stage_reached_tx, stage_reached_rx) = tokio::sync::oneshot::channel::<()>();
    let stage_reached_slot = Arc::new(Mutex::new(Some(stage_reached_tx)));

    // Owned recheck state only: the closure remains Send + Sync + 'static compatible. If caller
    // cancellation arrives before the third check, no later fixture borrow can keep this alive.
    let recheck_pairing_store = fixture.pairing_store.clone();
    let recheck_pairing_dir = fixture.pairing_dir.clone();
    let recheck_nonce = mcp.connection_nonce().to_string();
    let recheck_epoch = mcp.epoch_id().to_string();
    let recheck_binding = fixture.binding_a.clone();
    let recheck_calls_for_closure = Arc::clone(&recheck_calls);
    let holder_slot_for_closure = Arc::clone(&holder_slot);
    let stage_reached_slot_for_closure = Arc::clone(&stage_reached_slot);

    let recheck = move || {
        let attempt = recheck_calls_for_closure.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 3 {
            let guard = acquire_cross_process_lock(
                &recheck_pairing_dir,
                "pairing.lock",
                "synthetic canceled final enrollment recheck",
            )
            .map_err(|err| {
                crate::approved_mcp::format_approved_scoped_error(
                    &recheck_binding,
                    "start_approved_task",
                    err.code(),
                    "Synthetic final enrollment recheck could not acquire the pairing lock.",
                )
            })?;

            let holder = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(700));
                drop(guard);
            });
            {
                let mut slot = holder_slot_for_closure
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *slot = Some(holder);
            }

            // Signal only after the real pairing.lock is held and its finite holder thread is
            // running. The callback then blocks in enrollment verification until the holder
            // releases, so the actor can start the 100ms cancellation window at the intended stage.
            if let Some(sender) = stage_reached_slot_for_closure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
        }

        recheck_pairing_store
            .verify_enrolled_binding(&recheck_nonce, &recheck_epoch)
            .map(|_| ())
            .map_err(|err| {
                crate::approved_mcp::format_approved_scoped_error(
                    &recheck_binding,
                    "start_approved_task",
                    err.code(),
                    "Synthetic final enrollment recheck failed.",
                )
            })
    };

    let started = Instant::now();
    let mut request_handle = tokio::spawn(execute_start_approved_task_with_launcher(
        Arc::clone(&fixture.authority),
        fixture.sessions_dir.clone(),
        fixture.binding_a.clone(),
        THREAD_UUID_A.to_string(),
        StartApprovedTaskParams {
            approval_id: approval.approval_id.clone(),
            task_id: "task-root-agy".to_string(),
        },
        Some(Arc::new(ApprovedHostLauncher::new(policy))),
        recheck,
    ));

    // Wait asynchronously for the third callback to reach the held-pairing stage. A blocking
    // recheck must not be able to deadlock this actor on a current-thread runtime, and the bounded
    // wait makes a missed stage explicit instead of letting the 100ms timer expire early.
    let stage_signal = tokio::time::timeout(Duration::from_millis(1_000), stage_reached_rx).await;
    let stage_elapsed = started.elapsed();
    let stage_reached = matches!(stage_signal, Ok(Ok(())));

    // Start caller cancellation only now, while the third recheck holds pairing.lock for 700ms.
    let cancel_started = Instant::now();
    let cancel_result = tokio::time::timeout(Duration::from_millis(100), &mut request_handle).await;
    let cancel_elapsed = cancel_started.elapsed();
    let elapsed_at_cancellation = started.elapsed();
    let count_at_cancellation = read_invocation_count(&spy_count);

    // Drop the owned request if it is still pending at the cancellation boundary, then await its
    // cleanup before any late-spawn observation.
    if cancel_result.is_err() {
        request_handle.abort();
        let _request_cleanup = request_handle.await;
    }

    // Allow the fixed 700ms synthetic lock hold to finish before the late spawn check.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let count_at_late_check = read_invocation_count(&spy_count);

    let holder = holder_slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    if let Some(holder) = holder {
        tokio::task::spawn_blocking(move || {
            holder
                .join()
                .expect("synthetic pairing.lock holder must not panic");
        })
        .await
        .expect("pairing.lock holder join task must complete");
    }

    // One more bounded grace window after the holder has definitely exited and any pending owned
    // request has been aborted, to catch a detached spawn scheduled by cancellation cleanup.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let count_after_release = read_invocation_count(&spy_count);

    assert!(
        recheck_calls.load(Ordering::SeqCst) >= 3,
        "test must reach the final enrollment recheck before asserting canceled behavior"
    );
    assert!(
        stage_reached,
        "third enrollment recheck must acquire pairing.lock and start its finite holder before \
         cancellation (stage wait: {stage_elapsed:?})"
    );
    assert!(
        cancel_result.is_err(),
        "owned launcher request must still be pending after the 100ms caller-cancellation window \
         (elapsed: {cancel_elapsed:?})"
    );
    assert!(
        cancel_elapsed < Duration::from_millis(1_000),
        "100ms caller-cancellation window must not stretch toward the 2s startup deadline \
         (elapsed: {cancel_elapsed:?})"
    );
    assert!(
        elapsed_at_cancellation < Duration::from_millis(1_500),
        "caller cancellation must occur before the 2s startup deadline can expire \
         (elapsed: {elapsed_at_cancellation:?})"
    );
    assert_eq!(
        count_at_cancellation, 0,
        "caller cancellation must not spawn threadlane-host immediately"
    );
    assert_eq!(
        count_at_late_check, 0,
        "caller cancellation must not spawn threadlane-host by the late ~900ms check"
    );
    assert_eq!(
        count_after_release, 0,
        "caller cancellation must not become a detached spawn before the 2s startup deadline"
    );
}

/// A held per-binding conversation-change advancement lock
/// during direct MCP `start_approved_task` admission must respect the approved-host startup deadline
/// instead of blocking for the full duration of the lock holder, and must never spawn `threadlane-host`
/// or a worker before or after the lock is released.
#[tokio::test]
async fn initial_conversation_change_lock_is_bounded_by_startup_deadline_and_never_spawns_late() {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-conversation-change-lock-deadline",
        &fixture.binding_a,
        default_plan_tasks("Synthetic conversation-change lock deadline regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-conversation-change-lock-deadline",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_300_000,
    );

    let spy_exe = fixture.root.join("bin/spy-change-lock-host");
    let spy_count = fixture.root.join("spy-change-lock-count.txt");
    let spy_argv = fixture.root.join("spy-change-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-change-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic change-lock host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create change-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach change-lock test launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let plans_dir = fixture.authority.plans().dir().to_path_buf();
    let binding_for_lock = fixture.binding_a.clone();
    let guard = crate::conversation_change::acquire_conversation_advancement_guard(
        &plans_dir,
        &binding_for_lock,
    )
    .await
    .expect("acquire the actual per-binding advancement lock");
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel::<()>();
    let change_lock_holder = std::thread::spawn(move || {
        let _ = locked_tx.send(());
        std::thread::sleep(Duration::from_millis(800));
        drop(guard);
    });
    tokio::time::timeout(Duration::from_millis(500), locked_rx)
        .await
        .expect("synthetic conversation-change lock holder must signal acquisition")
        .expect("synthetic conversation-change lock holder must acquire before timing out");

    let started = Instant::now();
    let request_result = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await;
    let elapsed = started.elapsed();

    tokio::task::spawn_blocking(move || {
        change_lock_holder
            .join()
            .expect("synthetic conversation-change lock holder must not panic");
    })
    .await
    .expect("conversation-change lock holder join task must complete");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let err_result = request_result
        .expect_err("start_approved_task must fail closed while conversation-change lock is held");
    let err_payload = err_result
        .structured_content
        .expect("structured error payload");
    assert_eq!(err_payload["error"]["code"], "host_not_ready");
    assert!(
        elapsed < Duration::from_millis(500),
        "conversation-change lock wait must respect the 250ms startup deadline (elapsed: {elapsed:?})"
    );
    assert_eq!(
        read_invocation_count(&spy_count),
        0,
        "a timed-out conversation-change lock wait must never spawn threadlane-host, before or after release"
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        0,
        "no approved worker task may be dispatched without a started host"
    );
}

/// Holding `pairing.lock` at the supported `start_approved_task` request boundary (both via
/// `invoke_tool_json` and direct `#[tool] start_approved_task`) must keep current-thread Tokio
/// executor timers firing, fail closed within the `250ms` startup deadline, and never spawn a
/// host or worker before or after `pairing.lock` is released.
#[tokio::test]
async fn initial_pairing_lock_is_bounded_by_startup_deadline_keeps_timers_running_and_never_spawns_late(
) {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-initial-pairing-lock-deadline",
        &fixture.binding_a,
        default_plan_tasks("Synthetic initial pairing lock deadline regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-initial-pairing-lock-deadline",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_400_000,
    );
    let record = fixture
        .authority
        .get_for_operator(&approval.approval_id)
        .expect("load approval record");
    let canonical_sessions_root =
        fs::canonicalize(&fixture.sessions_dir).expect("canonicalize sessions_dir");

    let spy_exe = fixture.root.join("bin/spy-initial-pairing-lock-host");
    let spy_count = fixture.root.join("spy-initial-pairing-lock-count.txt");
    let spy_argv = fixture.root.join("spy-initial-pairing-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-initial-pairing-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic initial pairing-lock host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create initial pairing-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach initial pairing-lock test launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    for use_direct_method in [false, true] {
        let pairing_dir = fixture.pairing_dir.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel::<()>();
        let pairing_holder = std::thread::spawn(move || {
            let guard = acquire_cross_process_lock(
                &pairing_dir,
                "pairing.lock",
                "synthetic initial pairing.lock contention",
            )
            .expect("acquire exclusive pairing.lock on helper thread");
            let _ = locked_tx.send(());
            std::thread::sleep(Duration::from_millis(800));
            drop(guard);
        });
        tokio::time::timeout(Duration::from_millis(500), locked_rx)
            .await
            .expect("synthetic pairing.lock holder must signal acquisition")
            .expect("synthetic pairing.lock holder must acquire before timing out");

        let timer_ticks = Arc::new(AtomicUsize::new(0));
        let stop_ticker = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ticks_writer = Arc::clone(&timer_ticks);
        let stop_reader = Arc::clone(&stop_ticker);
        let ticker_task = tokio::spawn(async move {
            while !stop_reader.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(25)).await;
                ticks_writer.fetch_add(1, Ordering::AcqRel);
            }
        });

        let started = Instant::now();
        let request_result = if use_direct_method {
            mcp.start_approved_task(Parameters(StartApprovedTaskParams {
                approval_id: approval.approval_id.clone(),
                task_id: "task-root-agy".to_string(),
            }))
            .await
        } else {
            mcp.invoke_tool_json(
                "start_approved_task",
                json!({
                    "approval_id": approval.approval_id,
                    "task_id": "task-root-agy"
                }),
                None,
            )
            .await
        };
        let elapsed = started.elapsed();

        stop_ticker.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_millis(500), ticker_task)
            .await
            .expect("timer ticker task must stop within 500ms")
            .expect("timer ticker task must not panic");
        let ticks_during_call = timer_ticks.load(Ordering::Acquire);

        tokio::task::spawn_blocking(move || {
            pairing_holder
                .join()
                .expect("synthetic pairing.lock holder must not panic");
        })
        .await
        .expect("pairing.lock holder join task must complete");

        tokio::time::sleep(Duration::from_millis(250)).await;

        let err_result = request_result
            .expect_err("start_approved_task must fail closed while pairing.lock is held");
        let err_payload = err_result
            .structured_content
            .expect("structured error payload");
        assert_eq!(err_payload["error"]["code"], "host_not_ready");
        assert!(
            elapsed >= Duration::from_millis(200),
            "initial pairing.lock wait must expire near the 250ms startup deadline \
             (use_direct_method={use_direct_method}, elapsed={elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "initial pairing.lock wait must respect the 250ms startup deadline \
             (use_direct_method={use_direct_method}, elapsed={elapsed:?})"
        );
        assert!(
            ticks_during_call >= 3,
            "current-thread Tokio executor timers must keep running while pairing.lock is held \
             (use_direct_method={use_direct_method}, ticks={ticks_during_call})"
        );
        assert_eq!(
            read_invocation_count(&spy_count),
            0,
            "a timed-out pairing.lock wait must never spawn threadlane-host, before or after release"
        );
        assert_eq!(
            read_invocation_count(&fixture.agy_count_file),
            0,
            "no approved worker task may be dispatched without a started host"
        );
        assert!(
            read_host_launch_intent_optional(&canonical_sessions_root, &record)
                .expect("read launch intent after pairing.lock release")
                .is_none(),
            "timed-out pairing.lock precheck must never persist a late launch intent"
        );
    }
    mcp.wait_for_observation_journal_idle_for_test().await;
}

/// On the managed desktop route (`require_call_identity == true`), offloading pairing-store I/O
/// and metadata refresh must still enforce the caller's task-local `CALL_THREAD_HINT` on the
/// original async task across the initial precheck and every launcher enrollment recheck, reject
/// missing or forged hints (even when untrusted `_meta.threadId` claims the enrolled chat), and
/// cap slow metadata refreshes to the `250ms` startup deadline.
#[tokio::test]
async fn managed_route_start_approved_task_enforces_task_local_thread_identity_across_launcher_rechecks(
) {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationActorEvidence, ObservationCallState,
        ObservationRecordPhase,
    };

    let fixture = setup_approved_mcp_fixture().await;

    let (rev_live, digest_live) = create_published_plan(
        &fixture,
        "plan-managed-identity-live",
        &fixture.binding_a,
        default_plan_tasks("Managed route task-local identity regression"),
    );
    let approval_live = approve_published_plan(
        &fixture,
        "plan-managed-identity-live",
        rev_live,
        &digest_live,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_500_000,
    );
    let record_live = fixture
        .authority
        .get_for_operator(&approval_live.approval_id)
        .expect("load live approval record");
    let canonical_sessions_root =
        fs::canonicalize(&fixture.sessions_dir).expect("canonicalize sessions_dir");

    let host_script = fixture.root.join("bin/managed-identity-host");
    let host_count_file = fixture.root.join("managed-identity-host-count.txt");
    let host_argv_file = fixture.root.join("managed-identity-host-argv.txt");
    let host_pid_file = fixture.root.join("managed-identity-host-pid.txt");
    write_spy_host_script(
        &host_script,
        &host_count_file,
        &host_argv_file,
        &host_pid_file,
        0,
        Some("2"),
    );

    let argv_watch = host_argv_file.clone();
    let count_watch = host_count_file.clone();
    let stop_watcher = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_watcher_bg = Arc::clone(&stop_watcher);
    let (host_handle_tx, host_handle_rx) = tokio::sync::oneshot::channel();
    let watcher_task = tokio::spawn(async move {
        let deadline = Instant::now() + Duration::from_secs(4);
        while Instant::now() < deadline && !stop_watcher_bg.load(Ordering::Acquire) {
            if read_invocation_count(&count_watch) >= 1 {
                if let Ok(raw_argv) = fs::read_to_string(&argv_watch) {
                    let lines: Vec<String> = raw_argv.lines().map(str::to_string).collect();
                    if lines.len() == 12 {
                        let mut full_args = vec!["threadlane-host".to_string()];
                        full_args.extend(lines);
                        let parsed = HostCli::try_parse_from(&full_args)
                            .expect("spawned argv must parse as valid HostCli");
                        match parsed.command {
                            HostCommand::Serve(serve_args) => {
                                let cfg = serve_args
                                    .into_host_config()
                                    .expect("valid ApprovedHostConfig from spawned argv")
                                    .with_tick_interval(Duration::from_millis(20));
                                let handle = ApprovedHost::start_fresh_only(cfg)
                                    .await
                                    .expect("start_fresh_only from spawned argv");
                                let _ = host_handle_tx.send(handle);
                                return;
                            }
                            HostCommand::PrepareDependencies(_) => {
                                panic!("host startup must not prepare dependencies");
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let policy = ApprovedHostLauncherPolicy::new(&host_script).expect("validate host script");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create managed-route test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    mcp.record_test_observation_context_with_selection(
        &fixture.binding_a,
        THREAD_UUID_A,
        "Managed Closure Chat",
        &fixture.repo_dir,
        Some("proj-m4-approved"),
        Some("GPT-5.4"),
        Some("high"),
    );
    let repo_cwd_str = fixture.repo_dir.to_string_lossy().into_owned();
    mcp.set_test_observation_metadata_refresher(move |requested_thread| {
        assert_eq!(requested_thread, THREAD_UUID_A);
        Ok(crate::NativeThread {
            id: THREAD_UUID_A.to_string(),
            project_id: Some("proj-m4-approved".to_string()),
            title: Some("Managed Closure Chat".to_string()),
            cwd: repo_cwd_str.clone(),
            status: "idle".to_string(),
            model: Some("GPT-5.4".to_string()),
            effort: Some("high".to_string()),
            can_accept_direct_input: Some(true),
        })
    });

    // 1. Missing task-local `CALL_THREAD_HINT` must reject immediately even if caller `_meta`
    //    claims `THREAD_UUID_A`.
    let missing_err = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval_live.approval_id,
                "task_id": "task-root-agy"
            }),
            Some(json!({ "threadId": THREAD_UUID_A })),
        )
        .await
        .expect_err("missing CALL_THREAD_HINT on managed route must be rejected");
    assert_eq!(
        missing_err.structured_content.expect("missing err payload")["error"]["code"],
        "shared_connection_rejected"
    );
    assert_eq!(read_invocation_count(&host_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record_live)
            .expect("inspect launch intent after missing hint")
            .is_none()
    );

    // 2. Forged/mismatched task-local `CALL_THREAD_HINT` (`THREAD_UUID_B`) must reject immediately.
    let forged_err = crate::auto_pair::CALL_THREAD_HINT
        .scope(
            Some(THREAD_UUID_B.to_string()),
            mcp.invoke_tool_json(
                "start_approved_task",
                json!({
                    "approval_id": approval_live.approval_id,
                    "task_id": "task-root-agy"
                }),
                Some(json!({ "threadId": THREAD_UUID_A })),
            ),
        )
        .await
        .expect_err("mismatched CALL_THREAD_HINT on managed route must be rejected");
    assert_eq!(
        forged_err.structured_content.expect("forged err payload")["error"]["code"],
        "shared_connection_rejected"
    );
    assert_eq!(read_invocation_count(&host_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record_live)
            .expect("inspect launch intent after forged hint")
            .is_none()
    );

    // 3. Matching task-local `CALL_THREAD_HINT` (`THREAD_UUID_A`) succeeds across the initial
    //    enrollment check and all 6 launcher `run_enrollment_recheck_within_deadline` calls.
    let valid_res = crate::auto_pair::CALL_THREAD_HINT
        .scope(
            Some(THREAD_UUID_A.to_string()),
            mcp.invoke_tool_json(
                "start_approved_task",
                json!({
                    "approval_id": approval_live.approval_id,
                    "task_id": "task-root-agy"
                }),
                None,
            ),
        )
        .await
        .expect("matching CALL_THREAD_HINT must start host and dispatch approved task");
    let snap: TaskStateSnapshot = serde_json::from_value(
        valid_res.structured_content.expect("valid start payload")["data"].clone(),
    )
    .expect("decode TaskStateSnapshot");
    assert_eq!(snap.approval_id, approval_live.approval_id);
    assert_eq!(snap.task_id, "task-root-agy");
    assert!(snap.run_id.is_some());
    assert_eq!(read_invocation_count(&host_count_file), 1);

    let host_handle = host_handle_rx
        .await
        .expect("receive started host handle for managed-route check");
    stop_watcher.store(true, Ordering::Release);
    watcher_task.await.expect("join watcher task");

    let wait_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let task_res = crate::auto_pair::CALL_THREAD_HINT
            .scope(
                Some(THREAD_UUID_A.to_string()),
                mcp.invoke_tool_json(
                    "approved_task",
                    json!({
                        "approval_id": approval_live.approval_id,
                        "task_id": "task-root-agy"
                    }),
                    None,
                ),
            )
            .await
            .expect("poll approved_task on managed route");
        let task_snap: TaskStateSnapshot = serde_json::from_value(
            task_res.structured_content.expect("task payload")["data"].clone(),
        )
        .expect("decode polled TaskStateSnapshot");
        if task_snap.status == TaskExecutionStatus::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "timed out waiting for managed-route task to reach ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let (records, warnings) =
        read_journal_records_readonly(fixture.authority.plans().dir()).expect("read journal");
    assert!(warnings.is_empty());
    let start_records: Vec<_> = records
        .iter()
        .filter(|rec| rec.operation == "mcp.paired.start_approved_task")
        .collect();
    assert_eq!(start_records.len(), 2);
    assert_eq!(start_records[0].phase, ObservationRecordPhase::Entry);
    assert_eq!(start_records[1].phase, ObservationRecordPhase::Finish);
    assert_eq!(start_records[1].state, ObservationCallState::Completed);
    for rec in &start_records {
        let actor = rec.actor.as_ref().expect("paired coworker actor");
        assert_eq!(actor.evidence, ObservationActorEvidence::Configured);
        assert_eq!(actor.selected_model.as_deref(), Some("GPT-5.4"));
        assert_eq!(actor.selected_effort.as_deref(), Some("high"));
    }

    host_handle.shutdown().await.expect("shutdown host_handle");

    // 4. Slow metadata refresh + held `authority.lock` on the managed route must still honor the
    //    single 250ms `startup_deadline` while executor timers keep running, and never spawn late.
    let (rev_bounded, digest_bounded) = create_published_plan(
        &fixture,
        "plan-managed-identity-bounded",
        &fixture.binding_a,
        default_plan_tasks("Managed route bounded metadata refresh regression"),
    );
    let approval_bounded = approve_published_plan(
        &fixture,
        "plan-managed-identity-bounded",
        rev_bounded,
        &digest_bounded,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_550_000,
    );
    let record_bounded = fixture
        .authority
        .get_for_operator(&approval_bounded.approval_id)
        .expect("load bounded approval record");

    let spy_bounded_exe = fixture.root.join("bin/spy-managed-bounded-host");
    let spy_bounded_count = fixture.root.join("spy-managed-bounded-count.txt");
    let spy_bounded_argv = fixture.root.join("spy-managed-bounded-argv.txt");
    let spy_bounded_pid = fixture.root.join("spy-managed-bounded-pid.txt");
    write_spy_host_script(
        &spy_bounded_exe,
        &spy_bounded_count,
        &spy_bounded_argv,
        &spy_bounded_pid,
        0,
        None,
    );

    let bounded_policy = ApprovedHostLauncherPolicy::new(&spy_bounded_exe)
        .expect("valid bounded host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp_bounded = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create bounded managed-route MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(bounded_policy)
        .expect("attach bounded launcher policy");
    enroll_server(&fixture, &mcp_bounded, &fixture.binding_a, THREAD_UUID_A).await;

    mcp_bounded.record_test_observation_context_with_selection(
        &fixture.binding_a,
        THREAD_UUID_A,
        "Managed Bounded Chat",
        &fixture.repo_dir,
        Some("proj-m4-approved"),
        Some("GPT-5.4"),
        Some("high"),
    );
    let repo_cwd_bounded = fixture.repo_dir.to_string_lossy().into_owned();
    mcp_bounded.set_test_observation_metadata_refresher(move |_| {
        std::thread::sleep(Duration::from_millis(180));
        Ok(crate::NativeThread {
            id: THREAD_UUID_A.to_string(),
            project_id: Some("proj-m4-approved".to_string()),
            title: Some("Managed Bounded Chat".to_string()),
            cwd: repo_cwd_bounded.clone(),
            status: "idle".to_string(),
            model: Some("GPT-5.4".to_string()),
            effort: Some("high".to_string()),
            can_accept_direct_input: Some(true),
        })
    });

    let authority_lock_path = fixture.authority.lock_path().to_path_buf();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel::<()>();
    let authority_holder = std::thread::spawn(move || {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&authority_lock_path)
            .expect("open authority.lock for managed-route bounded check");
        file.lock_exclusive()
            .expect("acquire exclusive authority.lock");
        let _ = locked_tx.send(());
        std::thread::sleep(Duration::from_millis(800));
        drop(file);
    });
    tokio::time::timeout(Duration::from_millis(500), locked_rx)
        .await
        .expect("authority.lock holder must signal acquisition")
        .expect("authority.lock holder must acquire before timing out");

    let timer_ticks = Arc::new(AtomicUsize::new(0));
    let stop_ticker = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ticks_writer = Arc::clone(&timer_ticks);
    let stop_reader = Arc::clone(&stop_ticker);
    let ticker_task = tokio::spawn(async move {
        while !stop_reader.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            ticks_writer.fetch_add(1, Ordering::AcqRel);
        }
    });

    let started = Instant::now();
    let bounded_result = crate::auto_pair::CALL_THREAD_HINT
        .scope(
            Some(THREAD_UUID_A.to_string()),
            mcp_bounded.invoke_tool_json(
                "start_approved_task",
                json!({
                    "approval_id": approval_bounded.approval_id,
                    "task_id": "task-root-agy"
                }),
                None,
            ),
        )
        .await;
    let bounded_elapsed = started.elapsed();

    stop_ticker.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_millis(500), ticker_task)
        .await
        .expect("managed-route timer ticker must stop within 500ms")
        .expect("managed-route timer ticker must not panic");
    let bounded_ticks = timer_ticks.load(Ordering::Acquire);

    tokio::task::spawn_blocking(move || {
        authority_holder
            .join()
            .expect("managed-route authority.lock holder must not panic");
    })
    .await
    .expect("managed-route authority.lock join must complete");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let bounded_err =
        bounded_result.expect_err("slow metadata refresh + held authority.lock must fail closed");
    assert_eq!(
        bounded_err.structured_content.expect("bounded err payload")["error"]["code"],
        "host_not_ready"
    );
    assert!(
        bounded_elapsed < Duration::from_millis(500),
        "managed-route metadata refresh + startup wait must respect the 250ms deadline \
         (elapsed: {bounded_elapsed:?})"
    );
    assert!(
        bounded_ticks >= 3,
        "executor timers must keep firing during slow metadata refresh (ticks: {bounded_ticks})"
    );
    assert_eq!(read_invocation_count(&spy_bounded_count), 0);
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record_bounded)
            .expect("inspect bounded launch intent")
            .is_none()
    );
    mcp.wait_for_observation_journal_idle_for_test().await;
    mcp_bounded
        .wait_for_observation_journal_idle_for_test()
        .await;
}

struct OwnedJournalFlockHolder {
    release_tx: Option<std::sync::mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl OwnedJournalFlockHolder {
    async fn acquire(journal_dir: &Path) -> Self {
        use threadlane_core::observation::OBSERVATION_JOURNAL_FILE_NAME;

        let journal_path = journal_dir.join(OBSERVATION_JOURNAL_FILE_NAME);
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(&journal_path)
                .expect("open observation-journal.jsonl on helper thread");
            fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o600))
                .expect("enforce 0600 on observation-journal.jsonl");
            file.lock_exclusive()
                .expect("acquire exclusive observation-journal.jsonl flock");
            let _ = locked_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
            let _ = file.unlock();
            drop(file);
        });
        tokio::time::timeout(Duration::from_millis(500), locked_rx)
            .await
            .expect("journal flock holder must signal acquisition within 500ms")
            .expect("journal flock holder must not drop handshake channel");
        Self {
            release_tx: Some(release_tx),
            join_handle: Some(handle),
        }
    }

    async fn release_and_join(mut self) {
        if let Some(tx) = self.release_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            tokio::task::spawn_blocking(move || {
                handle.join().expect("journal flock holder must not panic");
            })
            .await
            .expect("journal flock holder join task must complete");
        }
    }
}

impl Drop for OwnedJournalFlockHolder {
    fn drop(&mut self) {
        if let Some(tx) = self.release_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Holding an actual exclusive OS `flock` on `<plans_dir>/observation-journal.jsonl` during
/// `start_approved_task` must keep current-thread Tokio executor timers scheduling, fail closed
/// within the `250ms` startup deadline (`< 500ms`), produce zero host/provider/dispatch/intent
/// side effects before or after lock release, and drain the ordered `Entry -> Finish(Failed)` pair
/// cleanly before `TempDir` cleanup.
#[tokio::test]
async fn observation_journal_entry_lock_is_bounded_by_startup_deadline_keeps_timers_running_and_never_spawns_late(
) {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationCallState, ObservationRecordPhase,
    };

    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-journal-entry-lock-deadline",
        &fixture.binding_a,
        default_plan_tasks("Observation journal Entry lock deadline regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-journal-entry-lock-deadline",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_600_000,
    );
    let record = fixture
        .authority
        .get_for_operator(&approval.approval_id)
        .expect("load approval record");
    let canonical_sessions_root =
        fs::canonicalize(&fixture.sessions_dir).expect("canonicalize sessions_dir");

    let spy_exe = fixture.root.join("bin/spy-journal-entry-lock-host");
    let spy_count = fixture.root.join("spy-journal-entry-lock-count.txt");
    let spy_argv = fixture.root.join("spy-journal-entry-lock-argv.txt");
    let spy_pid = fixture.root.join("spy-journal-entry-lock-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid synthetic journal-lock host policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create journal-lock test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach journal-lock launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let journal_dir = fixture.authority.plans().dir().to_path_buf();
    let journal_holder = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    let entry_started_rx = mcp.arm_test_observation_entry_started_signal();

    let timer_ticks = Arc::new(AtomicUsize::new(0));
    let stop_ticker = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ticks_writer = Arc::clone(&timer_ticks);
    let stop_reader = Arc::clone(&stop_ticker);
    let ticker_task = tokio::spawn(async move {
        while !stop_reader.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            ticks_writer.fetch_add(1, Ordering::AcqRel);
        }
    });

    let started = Instant::now();
    let request_result = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await;
    let elapsed = started.elapsed();

    stop_ticker.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_millis(500), ticker_task)
        .await
        .expect("timer ticker task must stop within 500ms")
        .expect("timer ticker task must not panic");
    let ticks_during_call = timer_ticks.load(Ordering::Acquire);

    tokio::time::timeout(Duration::from_millis(200), entry_started_rx)
        .await
        .expect("journal worker must have started Entry while flock was held")
        .expect("entry started signal must not drop");

    // Verify zero effects BEFORE releasing the journal flock.
    assert_eq!(read_invocation_count(&spy_count), 0);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record)
            .expect("read launch intent before journal flock release")
            .is_none()
    );

    // Release immediately after the bounded response and await definite journal drain.
    journal_holder.release_and_join().await;
    tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("observation journal worker must drain within 2s after flock release");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    tokio::time::sleep(Duration::from_millis(250)).await;

    let err_result = request_result
        .expect_err("start_approved_task must fail closed while journal flock is held");
    let err_payload = err_result
        .structured_content
        .expect("structured error payload");
    assert_eq!(err_payload["error"]["code"], "host_not_ready");
    assert!(
        elapsed >= Duration::from_millis(200),
        "journal flock wait must expire near the 250ms startup deadline (elapsed: {elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "journal flock wait must respect the 250ms startup deadline (elapsed: {elapsed:?})"
    );
    assert!(
        ticks_during_call >= 3,
        "executor timers must keep firing while journal flock is held (ticks: {ticks_during_call})"
    );
    assert_eq!(
        read_invocation_count(&spy_count),
        0,
        "releasing journal flock after timeout must never spawn threadlane-host"
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        0,
        "releasing journal flock after timeout must never dispatch a worker"
    );
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record)
            .expect("read launch intent after journal flock release")
            .is_none(),
        "releasing journal flock after timeout must never persist a launch intent"
    );

    let (records, warnings) =
        read_journal_records_readonly(&journal_dir).expect("read drained observation journal");
    assert!(warnings.is_empty());
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].phase, ObservationRecordPhase::Entry);
    assert_eq!(records[0].state, ObservationCallState::Running);
    assert_eq!(records[1].phase, ObservationRecordPhase::Finish);
    assert_eq!(records[1].state, ObservationCallState::Failed);
    assert_eq!(records[0].request_id, records[1].request_id);
}

/// When `start_approved_task` has already obtained a proven `Ok(TaskStateSnapshot)` against a live
/// ready host and `observation-journal.jsonl` `flock` contention blocks `Finish`, caller wait must
/// still be bounded by the same `250ms` startup deadline (`< 500ms` rather than the `5s` default),
/// the caller must receive its real `Ok(TaskStateSnapshot)` (never turned into `host_not_ready` or
/// `Unknown`), and the background worker must durably persist `Finish(Completed)` in FIFO order
/// once the lock is released.
#[tokio::test]
async fn observation_journal_finish_lock_on_success_retains_ok_result_within_startup_deadline() {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationCallState, ObservationRecordPhase,
    };

    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-journal-finish-lock-success",
        &fixture.binding_a,
        default_plan_tasks("Observation journal Finish lock on success regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-journal-finish-lock-success",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_650_000,
    );

    let host = ApprovedHost::start(
        ApprovedHostConfig::new(
            fixture.registry_dir.clone(),
            fixture.plans_dir.clone(),
            fixture.authority_dir.clone(),
            fixture.sessions_dir.clone(),
            approval.approval_id.clone(),
        )
        .with_tick_interval(Duration::from_millis(20)),
    )
    .await
    .expect("start ApprovedHost for success-Finish contention test");

    let spy_exe = fixture.root.join("bin/spy-journal-finish-unused-host");
    let spy_count = fixture.root.join("spy-journal-finish-unused-count.txt");
    let spy_argv = fixture.root.join("spy-journal-finish-unused-argv.txt");
    let spy_pid = fixture.root.join("spy-journal-finish-unused-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid launcher policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create success-Finish test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach 250ms launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let journal_dir = fixture.authority.plans().dir().to_path_buf();
    let journal_dir_for_barrier = journal_dir.clone();
    let (before_finish_rx, resume_finish_tx) = mcp.arm_test_observation_before_finish_barrier();
    let finish_lock_task = tokio::spawn(async move {
        before_finish_rx
            .await
            .expect("finish_paired_observation_until must reach barrier after dispatch succeeds");
        let holder = OwnedJournalFlockHolder::acquire(&journal_dir_for_barrier).await;
        let _ = resume_finish_tx.send(());
        holder
    });

    let timer_ticks = Arc::new(AtomicUsize::new(0));
    let stop_ticker = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ticks_writer = Arc::clone(&timer_ticks);
    let stop_reader = Arc::clone(&stop_ticker);
    let ticker_task = tokio::spawn(async move {
        while !stop_reader.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            ticks_writer.fetch_add(1, Ordering::AcqRel);
        }
    });

    let started = Instant::now();
    let ok_result = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect("proven host dispatch success must survive Finish journal deadline");
    let elapsed = started.elapsed();

    stop_ticker.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_millis(500), ticker_task)
        .await
        .expect("timer ticker task must stop within 500ms")
        .expect("timer ticker task must not panic");
    let ticks_during_call = timer_ticks.load(Ordering::Acquire);

    let journal_holder = tokio::time::timeout(Duration::from_millis(500), finish_lock_task)
        .await
        .expect("finish lock helper task must have acquired flock")
        .expect("finish lock helper task must not panic");

    let snap: TaskStateSnapshot = serde_json::from_value(
        ok_result.structured_content.expect("structured ok payload")["data"].clone(),
    )
    .expect("decode TaskStateSnapshot");
    assert_eq!(snap.approval_id, approval.approval_id);
    assert_eq!(snap.task_id, "task-root-agy");
    assert!(snap.run_id.is_some());
    assert_eq!(read_invocation_count(&spy_count), 0);
    assert!(
        elapsed >= Duration::from_millis(200),
        "Finish wait must use remaining 250ms startup_deadline budget while flock is held (elapsed: {elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Finish wait on success must be bounded by the 250ms startup_deadline, not 5s default (elapsed: {elapsed:?})"
    );
    assert!(
        ticks_during_call >= 3,
        "executor timers must keep firing during contended Finish wait (ticks: {ticks_during_call})"
    );

    journal_holder.release_and_join().await;
    tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("observation journal worker must drain Finish(Completed) after flock release");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    let (records, warnings) =
        read_journal_records_readonly(&journal_dir).expect("read drained observation journal");
    assert!(warnings.is_empty());
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].phase, ObservationRecordPhase::Entry);
    assert_eq!(records[0].state, ObservationCallState::Running);
    assert_eq!(records[1].phase, ObservationRecordPhase::Finish);
    assert_eq!(records[1].state, ObservationCallState::Completed);
    assert_eq!(records[0].request_id, records[1].request_id);

    let wait_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let task_res = mcp
            .invoke_tool_json(
                "approved_task",
                json!({
                    "approval_id": approval.approval_id,
                    "task_id": "task-root-agy"
                }),
                None,
            )
            .await
            .expect("poll approved_task");
        let task_snap: TaskStateSnapshot =
            serde_json::from_value(task_res.structured_content.expect("payload")["data"].clone())
                .expect("decode TaskStateSnapshot");
        if task_snap.status == TaskExecutionStatus::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "timed out waiting for dispatched task to reach ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    mcp.wait_for_observation_journal_idle_for_test().await;
    host.shutdown().await.expect("shutdown ApprovedHost");
}

/// Dropping a request future while its `Entry` append has already `STARTED` (`EntryRunning`) must
/// eventually write exactly one matching ordered `Finish(Cancelled)` record after `Entry` (leaving
/// no `Running` orphan and causing zero host/worker/intent side effects), whereas dropping a
/// second request future whose `Entry` is still `EntryQueued` behind it must reclaim its tombstone
/// immediately and write nothing at all.
#[tokio::test]
async fn observation_journal_drop_during_started_entry_and_queued_entry_emits_ordered_terminal_without_running_orphan(
) {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationCallState, ObservationRecordPhase,
    };

    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-journal-drop-started-entry",
        &fixture.binding_a,
        default_plan_tasks("Drop during STARTED Entry regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-journal-drop-started-entry",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_001_700_000,
    );
    let record = fixture
        .authority
        .get_for_operator(&approval.approval_id)
        .expect("load approval record");
    let canonical_sessions_root =
        fs::canonicalize(&fixture.sessions_dir).expect("canonicalize sessions_dir");

    let spy_exe = fixture.root.join("bin/spy-journal-drop-started-host");
    let spy_count = fixture.root.join("spy-journal-drop-started-count.txt");
    let spy_argv = fixture.root.join("spy-journal-drop-started-argv.txt");
    let spy_pid = fixture.root.join("spy-journal-drop-started-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    // Use a 4s policy so caller cancellation is completely isolated from startup_deadline expiry.
    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid launcher policy")
        .with_startup_timeout(Duration::from_secs(4))
        .expect("4s startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create drop-during-STARTED MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach 4s launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    let journal_dir = fixture.authority.plans().dir().to_path_buf();
    let journal_holder = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    let entry_started_rx = mcp.arm_test_observation_entry_started_signal();

    let mut fut_started = Box::pin(mcp.invoke_tool_json(
        "start_approved_task",
        json!({
            "approval_id": approval.approval_id,
            "task_id": "task-root-agy"
        }),
        None,
    ));

    tokio::select! {
        biased;
        res = &mut fut_started => {
            panic!("fut_started must remain Pending while journal flock is held: {res:?}");
        }
        started = tokio::time::timeout(Duration::from_secs(1), entry_started_rx) => {
            started
                .expect("fut_started must reach EntryRunning within 1s")
                .expect("entry started signal must not close");
        }
    }
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 1);

    let mut fut_queued = Box::pin(mcp.invoke_tool_json(
        "start_approved_task",
        json!({
            "approval_id": approval.approval_id,
            "task_id": "task-root-agy"
        }),
        None,
    ));
    let queue_deadline = Instant::now() + Duration::from_secs(1);
    while mcp.active_observation_journal_slot_count_for_test() < 2 {
        assert!(
            futures_util::poll!(&mut fut_queued).is_pending(),
            "fut_queued must stay Pending behind held journal flock"
        );
        assert!(
            Instant::now() < queue_deadline,
            "timed out waiting for fut_queued to enqueue its Entry behind fut_started"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 2);

    // Dropping the still-queued future must reclaim its tombstone and slot immediately without I/O.
    drop(fut_queued);
    assert_eq!(
        mcp.active_observation_journal_slot_count_for_test(),
        1,
        "dropping a queued Entry must immediately reclaim its tombstone and reserved slot"
    );

    // Dropping the already-STARTED future before handle return must schedule its ordered Cancelled
    // Finish record without allowing any dispatch or host launch.
    drop(fut_started);

    journal_holder.release_and_join().await;
    tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("observation journal worker must drain after flock release");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(read_invocation_count(&spy_count), 0);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &record)
            .expect("inspect launch intent after drop during STARTED Entry")
            .is_none()
    );

    let (records, warnings) =
        read_journal_records_readonly(&journal_dir).expect("read drained observation journal");
    assert!(warnings.is_empty());
    assert_eq!(
        records.len(),
        2,
        "queued cancellation must write nothing and STARTED cancellation must write exactly [Entry, Finish]"
    );
    assert_eq!(records[0].phase, ObservationRecordPhase::Entry);
    assert_eq!(records[0].state, ObservationCallState::Running);
    assert_eq!(records[1].phase, ObservationRecordPhase::Finish);
    assert_eq!(records[1].state, ObservationCallState::Cancelled);
    assert_eq!(records[0].request_id, records[1].request_id);
    assert_eq!(
        records[1].resolve_in_flight_state(false),
        ObservationCallState::Cancelled
    );
}

/// Bounded queue overflow and quick `append_journal_record` failures must remain honest:
/// 1. When hard queue capacity is full, `Entry` admission is skipped without blocking the async
///    caller, without writing an `Entry` that lacks `Finish` ownership or an orphan `Finish`, and
///    without inventing `host_not_ready` on a valid dispatch.
/// 2. When `append_journal_record` fails quickly (e.g. `0644` permissions on
///    `observation-journal.jsonl`), `begin_paired_observation_until` never claims a persisted
///    `Entry`, never emits an orphan `Finish`, and preserves honest tool/authority results instead
///    of fabricating `host_not_ready`.
#[tokio::test]
async fn observation_journal_queue_overflow_and_append_failure_remain_honest() {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationCallState, ObservationRecordPhase,
        OBSERVATION_JOURNAL_FILE_NAME,
    };

    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-journal-overflow-and-io-honesty",
        &fixture.binding_a,
        default_plan_tasks("Queue overflow and append failure honesty regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-journal-overflow-and-io-honesty",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string(), "task-root-oc".to_string()],
        1_700_001_750_000,
    );

    let host = ApprovedHost::start(
        ApprovedHostConfig::new(
            fixture.registry_dir.clone(),
            fixture.plans_dir.clone(),
            fixture.authority_dir.clone(),
            fixture.sessions_dir.clone(),
            approval.approval_id.clone(),
        )
        .with_tick_interval(Duration::from_millis(20)),
    )
    .await
    .expect("start ApprovedHost for overflow/append-failure honesty test");

    let spy_exe = fixture.root.join("bin/spy-journal-overflow-unused-host");
    let spy_count = fixture.root.join("spy-journal-overflow-unused-count.txt");
    let spy_argv = fixture.root.join("spy-journal-overflow-unused-argv.txt");
    let spy_pid = fixture.root.join("spy-journal-overflow-unused-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid launcher policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create overflow/io-honesty test MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach 250ms launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    // 1. Hard queue capacity overflow (`max_capacity = 1`): occupy the single slot with an
    //    in-flight `list_approvals` Entry held on `observation-journal.jsonl` flock, then run
    //    `start_approved_task` (`task-root-agy`). It must succeed honestly without blocking on
    //    queue push and without writing any partial/orphan journal record.
    mcp.set_test_observation_journal_capacity(1);
    let journal_dir = fixture.authority.plans().dir().to_path_buf();
    let journal_holder = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    let entry_started_rx = mcp.arm_test_observation_entry_started_signal();

    let mcp_bg = mcp.clone();
    let blocker_call = tokio::spawn(async move {
        mcp_bg
            .invoke_tool_json("list_approvals", json!({}), None)
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), entry_started_rx)
        .await
        .expect("list_approvals Entry must start within 1s")
        .expect("entry started signal must not close");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 1);

    let overflow_res = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect("queue overflow must skip best-effort observation and still dispatch valid task");
    let overflow_snap: TaskStateSnapshot = serde_json::from_value(
        overflow_res.structured_content.expect("overflow payload")["data"].clone(),
    )
    .expect("decode overflow TaskStateSnapshot");
    assert_eq!(overflow_snap.task_id, "task-root-agy");
    assert!(overflow_snap.run_id.is_some());

    journal_holder.release_and_join().await;
    let blocker_res = tokio::time::timeout(Duration::from_secs(2), blocker_call)
        .await
        .expect("blocker list_approvals call must complete after flock release")
        .expect("blocker task must not panic");
    assert!(blocker_res.is_ok());

    tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal must drain after overflow check");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    let (records_after_overflow, warnings) =
        read_journal_records_readonly(&journal_dir).expect("read journal after overflow");
    assert!(warnings.is_empty());
    assert_eq!(
        records_after_overflow.len(),
        2,
        "only the admitted list_approvals call may persist [Entry, Finish]; overflowed call must write 0 records"
    );
    assert_eq!(
        records_after_overflow[0].operation,
        "mcp.paired.list_approvals"
    );
    assert_eq!(
        records_after_overflow[0].phase,
        ObservationRecordPhase::Entry
    );
    assert_eq!(
        records_after_overflow[1].operation,
        "mcp.paired.list_approvals"
    );
    assert_eq!(
        records_after_overflow[1].phase,
        ObservationRecordPhase::Finish
    );
    assert_eq!(
        records_after_overflow[1].state,
        ObservationCallState::Completed
    );

    // 2. Quick `append_journal_record` failure (`0644` permissions violate private-file check):
    //    must not claim a persisted Entry, must not invent `host_not_ready`, and must preserve
    //    both honest error codes (`approval_not_found`) and honest `Ok` task dispatch!
    let journal_path = journal_dir.join(OBSERVATION_JOURNAL_FILE_NAME);
    fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o644))
        .expect("make observation-journal.jsonl group/world-readable to trigger fast append Err");

    let missing_approval_err = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": "apr_0123456789abcdef0123456789abcdef",
                "task_id": "task-root-oc"
            }),
            None,
        )
        .await
        .expect_err("unknown approval must fail with approval_not_found");
    let missing_err_payload = missing_approval_err
        .structured_content
        .expect("missing approval error payload");
    assert_eq!(
        missing_err_payload["error"]["code"], "approval_not_found",
        "fast journal append failure must not mask real authority errors as host_not_ready"
    );

    let fast_io_ok = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval.approval_id,
                "task_id": "task-root-oc"
            }),
            None,
        )
        .await
        .expect("fast journal append failure must not invent host_not_ready on valid ready task");
    let oc_snap: TaskStateSnapshot = serde_json::from_value(
        fast_io_ok.structured_content.expect("oc ok payload")["data"].clone(),
    )
    .expect("decode oc TaskStateSnapshot");
    assert_eq!(oc_snap.task_id, "task-root-oc");
    assert!(oc_snap.run_id.is_some());

    tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal worker must be idle after fast append errors");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    // Restore `0600` permissions and verify no partial/orphan records were appended while corrupt
    // and that capacity=1 remains healthy for subsequent calls.
    fs::set_permissions(&journal_path, fs::Permissions::from_mode(0o600))
        .expect("restore 0600 permissions on observation-journal.jsonl");
    let (records_restored, warnings_restored) =
        read_journal_records_readonly(&journal_dir).expect("read restored journal");
    assert!(warnings_restored.is_empty());
    assert_eq!(
        records_restored.len(),
        2,
        "failed Entry appends must never persist partial records or orphan Finish records"
    );

    let wait_deadline = Instant::now() + Duration::from_secs(5);
    for task_id in ["task-root-agy", "task-root-oc"] {
        loop {
            let task_res = mcp
                .invoke_tool_json(
                    "approved_task",
                    json!({
                        "approval_id": approval.approval_id,
                        "task_id": task_id
                    }),
                    None,
                )
                .await
                .expect("poll approved_task after restoring journal");
            let task_snap: TaskStateSnapshot = serde_json::from_value(
                task_res.structured_content.expect("payload")["data"].clone(),
            )
            .expect("decode TaskStateSnapshot");
            if task_snap.status == TaskExecutionStatus::ReadyForReview {
                break;
            }
            assert!(
                Instant::now() < wait_deadline,
                "timed out waiting for {task_id} to reach ReadyForReview"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    mcp.wait_for_observation_journal_idle_for_test().await;
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);
    assert_eq!(read_invocation_count(&spy_count), 0);
    host.shutdown().await.expect("shutdown ApprovedHost");
}

struct WorkerSpawnFailurePhaseContext {
    mcp: PairedMcpServer,
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    journal_dir: PathBuf,
    approval_id: String,
    spy_count: PathBuf,
    agy_count_file: PathBuf,
    oc_count_file: PathBuf,
}

async fn run_worker_spawn_failure_phases(ctx: WorkerSpawnFailurePhaseContext) {
    use threadlane_core::observation::{
        read_journal_records_readonly, ObservationCallState, ObservationRecordPhase,
    };
    use tokio::time::timeout;

    use crate::mcp_pair::TestFinishAckOutcome;

    let WorkerSpawnFailurePhaseContext {
        mcp,
        authority,
        sessions_dir,
        journal_dir,
        approval_id,
        spy_count,
        agy_count_file,
        oc_count_file,
    } = ctx;

    // 1. Entry worker-spawn failure while `observation-journal.jsonl` is exclusively `flock`ed:
    //    must not perform inline disk I/O, must close `done_rx` and entry waiters, reclaim the
    //    single capacity slot, and preserve proven `Ok` task dispatch under `< 500ms`.
    let journal_holder_1 = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    mcp.set_test_observation_journal_fail_worker_spawn(true);
    let entry_started_rx = mcp.arm_test_observation_entry_started_signal();

    let entry_fail_started = Instant::now();
    let agy_res = mcp
        .invoke_tool_json(
            "start_approved_task",
            json!({
                "approval_id": approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect("Entry worker-spawn failure must preserve proven task dispatch");
    let entry_fail_elapsed = entry_fail_started.elapsed();
    assert!(
        entry_fail_elapsed < Duration::from_millis(500),
        "Entry worker-spawn failure must never block on held journal flock \
         (elapsed: {entry_fail_elapsed:?})"
    );
    assert!(
        entry_started_rx.await.is_err(),
        "entry_started signal must close without firing when worker launch fails"
    );
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);
    timeout(
        Duration::from_millis(200),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal must report idle immediately after failed Entry worker launch");

    assert_ne!(agy_res.is_error, Some(true));
    let expected_agy_is_error = agy_res.is_error;
    let expected_agy_payload = agy_res
        .structured_content
        .clone()
        .expect("agy ok structured payload");
    let agy_data = expected_agy_payload["data"].clone();
    let agy_snap: TaskStateSnapshot = serde_json::from_value(agy_data).expect("decode snapshot");
    assert_eq!(agy_snap.approval_id, approval_id);
    assert_eq!(agy_snap.task_id, "task-root-agy");
    let agy_run_id = agy_snap
        .run_id
        .clone()
        .expect("agy task dispatch must produce real run_id");
    let proven_agy_result = Ok(agy_res);

    journal_holder_1.release_and_join().await;
    let (records_after_entry_fail, warnings_1) =
        read_journal_records_readonly(&journal_dir).expect("read journal after Entry spawn fail");
    assert!(warnings_1.is_empty());
    assert!(
        records_after_entry_fail.is_empty(),
        "failed Entry worker launch must write zero journal records"
    );

    // 2. Stage-isolated Finish worker-spawn failure using the actual proven `Ok(agy_res)` from
    //    Phase 1 (no forged DTO): persist a fresh observation `Entry`, wait for the journal worker
    //    to go idle, hold the real exclusive `flock` on `observation-journal.jsonl`, and inject
    //    worker-spawn failure. Exercising the production `Finish` helper with the configured
    //    `250ms` deadline must return under `< 500ms`, close acknowledgement without inline disk
    //    I/O, reclaim the single capacity slot, report idle immediately, and leave the proven
    //    successful result completely unchanged.
    mcp.set_test_observation_journal_fail_worker_spawn(false);
    let finish_obs = mcp
        .begin_persisted_paired_observation_for_test(
            "start_approved_task",
            Some(&approval_id),
            Some("task-root-agy"),
        )
        .await;
    let finish_entry_request_id = finish_obs.request_id().to_string();
    timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal worker must go idle after persisting Finish-stage Entry");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 1);

    let journal_holder_2 = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    mcp.set_test_observation_journal_fail_worker_spawn(true);

    let finish_timeout = mcp
        .host_launcher_policy()
        .expect("configured 250ms launcher policy")
        .startup_timeout();
    assert_eq!(finish_timeout, Duration::from_millis(250));

    let finish_fail_started = Instant::now();
    let finish_ack = mcp
        .finish_paired_observation_until_for_test(
            finish_obs,
            Some(finish_fail_started + finish_timeout),
            &proven_agy_result,
        )
        .await;
    let finish_fail_elapsed = finish_fail_started.elapsed();
    assert!(
        finish_fail_elapsed < Duration::from_millis(500),
        "Finish worker-spawn failure must never block on held journal flock \
         (elapsed: {finish_fail_elapsed:?})"
    );
    assert_eq!(
        finish_ack,
        TestFinishAckOutcome::Closed,
        "Finish worker-spawn failure must close acknowledgement channel without inline disk I/O"
    );
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);
    timeout(
        Duration::from_millis(200),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal must report idle immediately after failed Finish worker launch");

    let preserved_agy_res = proven_agy_result
        .as_ref()
        .expect("proven successful dispatch result must remain Ok");
    assert_eq!(preserved_agy_res.is_error, expected_agy_is_error);
    let preserved_payload = preserved_agy_res
        .structured_content
        .as_ref()
        .expect("preserved agy structured payload");
    assert_eq!(preserved_payload, &expected_agy_payload);
    let preserved_data = preserved_payload["data"].clone();
    let preserved_snap: TaskStateSnapshot = serde_json::from_value(preserved_data).expect("snap");
    assert_eq!(preserved_snap.approval_id, approval_id);
    assert_eq!(preserved_snap.task_id, "task-root-agy");
    assert_eq!(preserved_snap.run_id.as_deref(), Some(agy_run_id.as_str()));

    journal_holder_2.release_and_join().await;
    let (records_after_finish_fail, warnings_2) =
        read_journal_records_readonly(&journal_dir).expect("read journal after Finish spawn fail");
    assert!(warnings_2.is_empty());
    assert_eq!(
        records_after_finish_fail.len(),
        1,
        "failed Finish worker launch must leave only the pre-persisted Entry record"
    );
    assert_eq!(
        records_after_finish_fail[0].phase,
        ObservationRecordPhase::Entry
    );
    assert_eq!(
        records_after_finish_fail[0].request_id,
        finish_entry_request_id
    );

    // 3. Non-Tokio OS thread `Drop` when worker launch fails while `observation-journal.jsonl` is
    //    exclusively `flock`ed: must never invoke synchronous `run_worker_loop` inline on Drop.
    mcp.set_test_observation_journal_fail_worker_spawn(false);
    let (drop_barrier_rx, _drop_barrier_resume_tx) =
        mcp.arm_test_observation_before_finish_result_barrier();
    let mcp_for_drop = mcp.clone();
    let mut drop_fut = Box::pin(async move {
        mcp_for_drop
            .invoke_tool_json("list_approvals", json!({}), None)
            .await
    });
    tokio::select! {
        biased;
        res = &mut drop_fut => {
            panic!("list_approvals must pause at Finish barrier before Drop: {res:?}");
        }
        reached = timeout(Duration::from_secs(1), drop_barrier_rx) => {
            let pre_finish_res = reached
                .expect("list_approvals must reach Finish barrier within 1s")
                .expect("drop barrier signal must not close");
            pre_finish_res.expect("list_approvals must succeed before Finish barrier");
        }
    }
    mcp.wait_for_observation_journal_idle_for_test().await;
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 1);

    let journal_holder_3 = OwnedJournalFlockHolder::acquire(&journal_dir).await;
    mcp.set_test_observation_journal_fail_worker_spawn(true);

    let mut drop_blocking_task = tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || {
            drop(drop_fut);
        })
        .join()
        .expect("non-Tokio Drop thread must not panic");
    });
    let drop_budget = Duration::from_millis(500);
    match timeout(drop_budget, &mut drop_blocking_task).await {
        Ok(join_res) => {
            join_res.expect("spawn_blocking wrapper must not panic");
        }
        Err(elapsed_err) => {
            mcp.set_test_observation_journal_fail_worker_spawn(false);
            journal_holder_3.release_and_join().await;
            // Bounded best-effort reap: Rust cannot forcibly cancel an already-running OS thread,
            // so wait at most 2s after releasing the flock and report if the thread detaches.
            let cleanup_note = match timeout(Duration::from_secs(2), drop_blocking_task).await {
                Ok(Ok(())) => "joined_ok".to_string(),
                Ok(Err(e)) if e.is_panic() => format!("joined_panic ({e})"),
                Ok(Err(e)) => format!("joined_error ({e})"),
                Err(e) => format!("timed_out ({e}); blocking OS thread may detach"),
            };
            panic!(
                "non-Tokio Drop must return without blocking on held journal flock \
                 ({elapsed_err}); post-unlock 2s cleanup={cleanup_note} \
                 (Rust cannot forcibly cancel an already-running OS thread)"
            );
        }
    }
    assert_eq!(
        mcp.active_observation_journal_slot_count_for_test(),
        0,
        "non-Tokio Drop on worker-spawn failure must reclaim slot immediately"
    );
    journal_holder_3.release_and_join().await;

    // 4. Clearing the injected failure with `max_capacity = 1` still active recovers cleanly.
    mcp.set_test_observation_journal_fail_worker_spawn(false);
    mcp.invoke_tool_json("list_approvals", json!({}), None)
        .await
        .expect("list_approvals must succeed once worker spawning recovers");
    timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await
    .expect("journal worker must drain recovered list_approvals pair");
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);

    let (records_recovered, warnings_recovered) =
        read_journal_records_readonly(&journal_dir).expect("read journal after recovery");
    assert!(warnings_recovered.is_empty());
    assert_eq!(
        records_recovered.len(),
        4,
        "only the two pre-failure Entries and recovered [Entry, Finish] pair may exist"
    );
    assert_eq!(records_recovered[0].phase, ObservationRecordPhase::Entry);
    assert_eq!(records_recovered[0].request_id, finish_entry_request_id);
    assert_eq!(records_recovered[1].phase, ObservationRecordPhase::Entry);
    assert_eq!(records_recovered[2].phase, ObservationRecordPhase::Entry);
    assert_eq!(records_recovered[3].phase, ObservationRecordPhase::Finish);
    assert_eq!(records_recovered[3].state, ObservationCallState::Completed);
    assert_eq!(
        records_recovered[2].request_id,
        records_recovered[3].request_id
    );

    let wait_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let task_res = mcp
            .invoke_tool_json(
                "approved_task",
                json!({
                    "approval_id": approval_id,
                    "task_id": "task-root-agy"
                }),
                None,
            )
            .await
            .expect("poll approved_task after worker-spawn failure recovery");
        let task_snap: TaskStateSnapshot =
            serde_json::from_value(task_res.structured_content.expect("payload")["data"].clone())
                .expect("decode TaskStateSnapshot");
        if task_snap.status == TaskExecutionStatus::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "timed out waiting for task-root-agy to reach ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let oc_task_res = mcp
        .invoke_tool_json(
            "approved_task",
            json!({
                "approval_id": approval_id,
                "task_id": "task-root-oc"
            }),
            None,
        )
        .await
        .expect("inspect undispatched task-root-oc after recovery");
    let oc_task_snap: TaskStateSnapshot =
        serde_json::from_value(oc_task_res.structured_content.expect("payload")["data"].clone())
            .expect("decode task-root-oc TaskStateSnapshot");
    assert!(
        oc_task_snap.run_id.is_none(),
        "stage-isolated Finish test must leave task-root-oc undispatched"
    );

    mcp.wait_for_observation_journal_idle_for_test().await;
    assert_eq!(mcp.active_observation_journal_slot_count_for_test(), 0);
    assert_eq!(read_invocation_count(&spy_count), 0);
    assert_eq!(read_invocation_count(&agy_count_file), 1);
    assert_eq!(read_invocation_count(&oc_count_file), 0);
    let approval_record = authority
        .get_for_operator(&approval_id)
        .expect("load approval record");
    let canonical_sessions_root =
        fs::canonicalize(&sessions_dir).expect("canonicalize sessions_dir");
    assert!(
        read_host_launch_intent_optional(&canonical_sessions_root, &approval_record)
            .expect("read launch intent after worker-spawn failure recovery")
            .is_none(),
        "worker-spawn failure recovery must never persist a host launch intent"
    );
}

/// If background observation-worker launch is unavailable (injected deterministically without OS
/// resource exhaustion), neither `Entry`, `Finish`, nor a plain non-Tokio `Drop` thread may fall
/// back to synchronous inline journal `flock`/`fsync`. Acknowledgements must close cleanly, slots
/// and bounded capacity must be reclaimed immediately without disk I/O, proven operation results
/// must be preserved within the `250ms` startup policy (`< 500ms`), and subsequent calls must
/// recover once worker spawning succeeds again.
#[tokio::test]
async fn observation_journal_worker_spawn_failure_never_blocks_inline_and_reclaims_capacity() {
    let fixture = setup_approved_mcp_fixture().await;

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-journal-worker-spawn-failure",
        &fixture.binding_a,
        default_plan_tasks("Observation worker spawn failure regression"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-journal-worker-spawn-failure",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string(), "task-root-oc".to_string()],
        1_700_001_800_000,
    );

    let host = ApprovedHost::start(
        ApprovedHostConfig::new(
            fixture.registry_dir.clone(),
            fixture.plans_dir.clone(),
            fixture.authority_dir.clone(),
            fixture.sessions_dir.clone(),
            approval.approval_id.clone(),
        )
        .with_tick_interval(Duration::from_millis(20)),
    )
    .await
    .expect("start ApprovedHost for worker-spawn failure test");

    let spy_exe = fixture.root.join("bin/spy-journal-spawn-fail-unused-host");
    let spy_count = fixture.root.join("spy-journal-spawn-fail-unused-count.txt");
    let spy_argv = fixture.root.join("spy-journal-spawn-fail-unused-argv.txt");
    let spy_pid = fixture.root.join("spy-journal-spawn-fail-unused-pid.txt");
    write_spy_host_script(&spy_exe, &spy_count, &spy_argv, &spy_pid, 0, None);

    let policy = ApprovedHostLauncherPolicy::new(&spy_exe)
        .expect("valid launcher policy")
        .with_startup_timeout(Duration::from_millis(250))
        .expect("250ms startup timeout");
    let mcp = PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create worker-spawn failure MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
        .with_host_launcher_policy(policy)
        .expect("attach 250ms launcher policy");
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;

    mcp.set_test_observation_journal_capacity(1);
    let journal_dir = fixture.authority.plans().dir().to_path_buf();

    let phase_ctx = WorkerSpawnFailurePhaseContext {
        mcp: mcp.clone(),
        authority: Arc::clone(&fixture.authority),
        sessions_dir: fixture.sessions_dir.clone(),
        journal_dir,
        approval_id: approval.approval_id.clone(),
        spy_count,
        agy_count_file: fixture.agy_count_file.clone(),
        oc_count_file: fixture.oc_count_file.clone(),
    };

    let phase_task = tokio::spawn(run_worker_spawn_failure_phases(phase_ctx));
    let phase_res = phase_task.await;
    mcp.set_test_observation_journal_fail_worker_spawn(false);
    // The idle wait is bounded best-effort: an already-running blocking thread may outlive a
    // failed phase. Preserve its original panic; require both cleanup results on phase success.
    let idle_cleanup_res = tokio::time::timeout(
        Duration::from_secs(2),
        mcp.wait_for_observation_journal_idle_for_test(),
    )
    .await;
    let shutdown_res = host.shutdown().await;

    match phase_res {
        Ok(()) => {
            if let Err(idle_err) = idle_cleanup_res {
                panic!(
                    "observation journal must finish draining within 2s \
                     ({idle_err}); host_shutdown={shutdown_res:?}"
                );
            }
            shutdown_res.expect("shutdown ApprovedHost");
        }
        Err(join_err) if join_err.is_panic() => {
            if let Err(idle_err) = idle_cleanup_res {
                eprintln!(
                    "worker-spawn-failure cleanup diagnostic: 2s journal idle wait \
                     timed out after phase panic ({idle_err}); blocking OS thread \
                     may detach"
                );
            }
            if let Err(shutdown_err) = shutdown_res {
                eprintln!(
                    "worker-spawn-failure cleanup diagnostic: ApprovedHost shutdown \
                     failed after phase panic ({shutdown_err})"
                );
            }
            std::panic::resume_unwind(join_err.into_panic());
        }
        Err(join_err) => {
            panic!(
                "worker-spawn failure phase task failed without panic ({join_err}); \
                 idle_cleanup={idle_cleanup_res:?}, host_shutdown={shutdown_res:?}"
            );
        }
    }
}

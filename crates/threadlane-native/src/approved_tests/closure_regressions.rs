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

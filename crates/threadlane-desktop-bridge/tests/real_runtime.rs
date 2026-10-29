//! End-to-end bridge checks against the actual local supervisor, without providers or Codex.
use std::time::Duration;

use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_desktop_bridge::{ConnectionState, DesktopBridge, DesktopSnapshot};

async fn wait_for_status(bridge: &DesktopBridge, expected: &str) -> DesktopSnapshot {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = bridge.snapshot(None).await.unwrap();
            if snapshot
                .tasks
                .first()
                .is_some_and(|task| task.status == expected)
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real supervisor should reach the expected demo status")
}

#[tokio::test]
async fn real_supervisor_demo_roundtrip_preserves_follower_cursors_and_external_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let state_dir = directory.path().join("state");
    let mut config = SupervisorConfig::new(&state_dir);
    config.tick_interval = Duration::from_millis(10);
    let supervisor = Supervisor::start(config).await.unwrap();
    let bridge = DesktopBridge::new(None, Some(state_dir.clone())).unwrap();
    let initial = bridge.snapshot(None).await.unwrap();
    assert_eq!(initial.attachment.connection, ConnectionState::Connected);
    assert!(initial.tasks.is_empty());

    bridge.start_demo().await.unwrap();
    let waiting = wait_for_status(&bridge, "waiting_input").await;
    let task = &waiting.tasks[0];
    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .unwrap()
        .scoped(task.binding_id.clone())
        .unwrap();
    let before = scoped.status().await.unwrap();
    for _ in 0..3 {
        let snapshot = bridge.snapshot(None).await.unwrap();
        assert_eq!(snapshot.active_runs, 1);
        let detail = bridge
            .run_detail(task.binding_id.clone(), task.run_id.clone())
            .await
            .unwrap();
        assert!(detail.question.is_some());
    }
    let after = scoped.status().await.unwrap();
    assert_eq!(before.binding.delivered_seq, after.binding.delivered_seq);
    assert_eq!(before.binding.acked_seq, after.binding.acked_seq);
    assert_eq!(before.wakeup, after.wakeup);

    bridge.answer_demo(task.revision).await.unwrap();
    assert!(bridge.answer_demo(task.revision).await.is_err());
    let completed = wait_for_status(&bridge, "completed").await;
    assert!(!completed.demo_controls.can_answer);
    assert!(!completed.demo_controls.can_cancel);

    let second = bridge.start_demo().await.unwrap();
    let revision = second.demo_controls.revision.unwrap();
    let cancelled = bridge.cancel_demo(revision).await.unwrap();
    assert_eq!(cancelled.tasks[0].status, "cancelled");
    assert!(bridge.cancel_demo(revision).await.is_err());

    // Closing/dropping the observer never owns or shuts down the attached runtime.
    drop(bridge);
    assert!(scoped.status().await.is_ok());
    supervisor.shutdown().await;
}

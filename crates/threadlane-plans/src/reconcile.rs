use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::compiler::CompiledPlan;
use crate::error::PlanError;
use crate::types::{PlanRevision, ReplacementState};
use crate::validation::validate_plan_revision;

/// Observed stop state of a task execution from the prior plan revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriorTaskStopStatus {
    /// Task was never started in the prior revision.
    NotStarted,
    /// Prior worker is still running or queued; cancellation has not yet been requested.
    Running,
    /// Cancellation was requested on the prior worker, which is not yet proven stopped.
    CancelRequested,
    /// Prior worker process group has exited and been reaped (or reached a proven terminal state).
    ProvenStopped,
    /// Prior worker stop state is uncertain (e.g., supervisor restart or lost supervision).
    Unknown,
}

/// Classification of how a task changed between two revisions of the same plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskChangeKind {
    /// Task specification and all transitive upstream dependencies are identical.
    Unchanged,
    /// Task did not exist in the prior revision and was added in the target revision.
    Added,
    /// Task existed in the prior revision and was removed in the target revision.
    Removed,
    /// Task's own instruction, model/role/difficulty choice, write scope, title, or direct dependencies changed.
    Modified,
    /// Task's own fields are unchanged, but an upstream dependency was modified.
    DependencyUpdated,
}

/// Pure reconciliation record tracking cancellation, proven stop, artifact preservation, and
/// replacement admission for a single task across plan revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReplacement {
    /// Logical plan identifier.
    pub plan_id: String,
    /// Source plan revision number.
    pub from_revision: u64,
    /// Target plan revision number.
    pub to_revision: u64,
    /// Task identifier.
    pub task_id: String,
    /// Classification of how the task changed between `from_revision` and `to_revision`.
    pub change_kind: TaskChangeKind,
    /// Sorted list of field categories that changed for this task.
    pub changed_fields: Vec<String>,
    /// Upstream dependency task IDs in `to_revision`.
    pub depends_on: Vec<String>,
    /// Current replacement lifecycle state (`pending`, `cancel_requested`, `stopped`, `admitted`, or `unknown`).
    pub state: ReplacementState,
    /// `true` only when `state == Stopped`, the task exists in `to_revision`, and all upstream
    /// dependencies in `to_revision` have already reached `Admitted`.
    pub admission_allowed: bool,
    /// `true` whenever the task existed in `from_revision` so prior worktrees/commits/logs are preserved.
    pub preserve_old_artifacts: bool,
    /// Always `false`: Threadlane never claims live in-flight steer was delivered to a worker process.
    pub live_steer_delivered: bool,
}

/// Pure reconciliation report between two revisions of the same plan.
///
/// Instruction or scope updates never directly cancel or spawn OS processes and never claim
/// in-flight live steering. Affected tasks must progress through proven stop before a replacement
/// can be admitted, and `unknown` stop outcomes block replacement admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReconciliationReport {
    /// Logical plan identifier.
    pub plan_id: String,
    /// Prior plan revision number.
    pub from_revision: u64,
    /// Target plan revision number.
    pub to_revision: u64,
    /// Ordered list of affected task IDs (`Added`, `Removed`, `Modified`, or `DependencyUpdated`).
    pub affected_task_ids: Vec<String>,
    /// Ordered list of completely unaffected task IDs (`Unchanged`).
    pub unchanged_task_ids: Vec<String>,
    /// Per-task replacement state records.
    pub replacements: Vec<TaskReplacement>,
    /// Always `false`: no in-flight live steer is ever claimed.
    pub live_steer_delivered: bool,
}

impl PlanReconciliationReport {
    /// Looks up a [`TaskReplacement`] entry by `task_id`.
    pub fn task_replacement(&self, task_id: &str) -> Option<&TaskReplacement> {
        self.replacements
            .iter()
            .find(|entry| entry.task_id == task_id)
    }
}

#[derive(Debug, Clone)]
struct ComparableTaskView {
    id: String,
    title: String,
    instruction: String,
    role_and_difficulty: String,
    resolved_choice: String,
    depends_on: Vec<String>,
    allowed_write_paths: Vec<String>,
}

/// Purely reconciles a [`CompiledPlan`] transition (`previous -> next`) against observed prior
/// task stop states.
///
/// Both `previous` and `next` must belong to the same `plan_id` and `binding_id`, with
/// `next.plan_revision > previous.plan_revision`.
pub fn reconcile_compiled_plans(
    previous: &CompiledPlan,
    next: &CompiledPlan,
    observed_prior_states: &BTreeMap<String, PriorTaskStopStatus>,
) -> Result<PlanReconciliationReport, PlanError> {
    if previous.plan_id != next.plan_id {
        return Err(PlanError::validation(
            "plan_id",
            format!(
                "cannot reconcile different plans (`{}` vs `{}`)",
                previous.plan_id, next.plan_id
            ),
        ));
    }
    if previous.binding_id != next.binding_id {
        return Err(PlanError::BindingMismatch {
            plan_id: previous.plan_id.clone(),
            expected_binding_id: previous.binding_id.clone(),
            actual_binding_id: next.binding_id.clone(),
        });
    }
    if next.plan_revision <= previous.plan_revision {
        return Err(PlanError::validation(
            "plan_revision",
            format!(
                "target plan revision ({}) must be strictly greater than previous revision ({})",
                next.plan_revision, previous.plan_revision
            ),
        ));
    }

    let prev_views: Vec<ComparableTaskView> = previous
        .tasks
        .iter()
        .map(|t| ComparableTaskView {
            id: t.id.clone(),
            title: t.title.clone(),
            instruction: t.instruction.clone(),
            role_and_difficulty: format!("{:?}:{:?}", t.role, t.difficulty),
            resolved_choice: format!(
                "{}:{:?}:{}:{:?}:{:?}:{}",
                t.choice.model_id,
                t.choice.effort,
                t.tool_id,
                t.adapter,
                t.executable_path,
                t.model_arg
            ),
            depends_on: t.depends_on.clone(),
            allowed_write_paths: t.allowed_write_paths.clone(),
        })
        .collect();

    let next_views: Vec<ComparableTaskView> = next
        .tasks
        .iter()
        .map(|t| ComparableTaskView {
            id: t.id.clone(),
            title: t.title.clone(),
            instruction: t.instruction.clone(),
            role_and_difficulty: format!("{:?}:{:?}", t.role, t.difficulty),
            resolved_choice: format!(
                "{}:{:?}:{}:{:?}:{:?}:{}",
                t.choice.model_id,
                t.choice.effort,
                t.tool_id,
                t.adapter,
                t.executable_path,
                t.model_arg
            ),
            depends_on: t.depends_on.clone(),
            allowed_write_paths: t.allowed_write_paths.clone(),
        })
        .collect();

    reconcile_task_views(
        &previous.plan_id,
        previous.plan_revision,
        next.plan_revision,
        &prev_views,
        &next_views,
        &next.topological_order,
        observed_prior_states,
    )
}

/// Purely reconciles two [`PlanRevision`] snapshots (`previous -> next`) against observed prior
/// task stop states.
///
/// Validates both revisions and requires `next.revision > previous.revision` on the same
/// `plan_id` and `binding_id`.
pub fn reconcile_plan_revisions(
    previous: &PlanRevision,
    next: &PlanRevision,
    observed_prior_states: &BTreeMap<String, PriorTaskStopStatus>,
) -> Result<PlanReconciliationReport, PlanError> {
    let _ = validate_plan_revision(previous)?;
    let next_topo = validate_plan_revision(next)?;

    if previous.plan_id != next.plan_id {
        return Err(PlanError::validation(
            "plan_id",
            format!(
                "cannot reconcile different plans (`{}` vs `{}`)",
                previous.plan_id, next.plan_id
            ),
        ));
    }
    if previous.binding_id != next.binding_id {
        return Err(PlanError::BindingMismatch {
            plan_id: previous.plan_id.clone(),
            expected_binding_id: previous.binding_id.clone(),
            actual_binding_id: next.binding_id.clone(),
        });
    }
    if next.revision <= previous.revision {
        return Err(PlanError::validation(
            "revision",
            format!(
                "target revision ({}) must be strictly greater than previous revision ({})",
                next.revision, previous.revision
            ),
        ));
    }

    let profile_changed = previous.profile_id != next.profile_id
        || previous.profile_revision != next.profile_revision;

    let prev_views: Vec<ComparableTaskView> = previous
        .tasks
        .iter()
        .map(|t| ComparableTaskView {
            id: t.id.clone(),
            title: t.title.clone(),
            instruction: t.instruction.clone(),
            role_and_difficulty: format!("{:?}:{:?}", t.role, t.difficulty),
            resolved_choice: format!("{}:{}", previous.profile_id, previous.profile_revision),
            depends_on: t.depends_on.clone(),
            allowed_write_paths: t.allowed_write_paths.clone(),
        })
        .collect();

    let next_views: Vec<ComparableTaskView> = next
        .tasks
        .iter()
        .map(|t| ComparableTaskView {
            id: t.id.clone(),
            title: t.title.clone(),
            instruction: t.instruction.clone(),
            role_and_difficulty: format!("{:?}:{:?}", t.role, t.difficulty),
            resolved_choice: if profile_changed {
                format!("{}:{}", next.profile_id, next.profile_revision)
            } else {
                format!("{}:{}", previous.profile_id, previous.profile_revision)
            },
            depends_on: t.depends_on.clone(),
            allowed_write_paths: t.allowed_write_paths.clone(),
        })
        .collect();

    reconcile_task_views(
        &previous.plan_id,
        previous.revision,
        next.revision,
        &prev_views,
        &next_views,
        &next_topo,
        observed_prior_states,
    )
}

fn reconcile_task_views(
    plan_id: &str,
    from_revision: u64,
    to_revision: u64,
    prev_views: &[ComparableTaskView],
    next_views: &[ComparableTaskView],
    next_topological_order: &[String],
    observed_prior_states: &BTreeMap<String, PriorTaskStopStatus>,
) -> Result<PlanReconciliationReport, PlanError> {
    let prev_by_id: BTreeMap<&str, &ComparableTaskView> =
        prev_views.iter().map(|t| (t.id.as_str(), t)).collect();
    let next_by_id: BTreeMap<&str, &ComparableTaskView> =
        next_views.iter().map(|t| (t.id.as_str(), t)).collect();

    let mut direct_changes: BTreeMap<String, (TaskChangeKind, Vec<String>)> = BTreeMap::new();

    for next_task in next_views {
        match prev_by_id.get(next_task.id.as_str()) {
            None => {
                direct_changes.insert(
                    next_task.id.clone(),
                    (TaskChangeKind::Added, vec!["added".to_string()]),
                );
            }
            Some(prev_task) => {
                let mut fields = Vec::new();
                if prev_task.title != next_task.title {
                    fields.push("title".to_string());
                }
                if prev_task.instruction != next_task.instruction {
                    fields.push("instruction".to_string());
                }
                if prev_task.role_and_difficulty != next_task.role_and_difficulty
                    || prev_task.resolved_choice != next_task.resolved_choice
                {
                    fields.push("choice".to_string());
                }
                if prev_task.depends_on != next_task.depends_on {
                    fields.push("depends_on".to_string());
                }
                if prev_task.allowed_write_paths != next_task.allowed_write_paths {
                    fields.push("allowed_write_paths".to_string());
                }

                if fields.is_empty() {
                    direct_changes.insert(
                        next_task.id.clone(),
                        (TaskChangeKind::Unchanged, Vec::new()),
                    );
                } else {
                    direct_changes.insert(next_task.id.clone(), (TaskChangeKind::Modified, fields));
                }
            }
        }
    }

    // Propagate upstream changes along the validated topological order of `next`.
    let mut affected_set: BTreeSet<String> = direct_changes
        .iter()
        .filter(|(_, (kind, _))| *kind != TaskChangeKind::Unchanged)
        .map(|(id, _)| id.clone())
        .collect();

    for task_id in next_topological_order {
        if let Some(next_task) = next_by_id.get(task_id.as_str()) {
            let upstream_affected = next_task
                .depends_on
                .iter()
                .any(|dep_id| affected_set.contains(dep_id));
            if upstream_affected {
                affected_set.insert(task_id.clone());
                if let Some((kind, fields)) = direct_changes.get_mut(task_id) {
                    if *kind == TaskChangeKind::Unchanged {
                        *kind = TaskChangeKind::DependencyUpdated;
                        fields.push("upstream_dependency".to_string());
                    }
                }
            }
        }
    }

    let mut replacements = Vec::new();
    let mut affected_task_ids = Vec::new();
    let mut unchanged_task_ids = Vec::new();

    // First include all tasks present in `next` in declaration order.
    for next_task in next_views {
        let (change_kind, changed_fields) = direct_changes
            .get(&next_task.id)
            .cloned()
            .unwrap_or((TaskChangeKind::Unchanged, Vec::new()));

        let existed_in_prev = prev_by_id.contains_key(next_task.id.as_str());
        let initial_state = match change_kind {
            TaskChangeKind::Unchanged => ReplacementState::Admitted,
            TaskChangeKind::Added => ReplacementState::Stopped,
            TaskChangeKind::Modified | TaskChangeKind::DependencyUpdated => {
                initial_replacement_state(observed_prior_states.get(&next_task.id).copied())
            }
            TaskChangeKind::Removed => unreachable!(),
        };

        if change_kind == TaskChangeKind::Unchanged {
            unchanged_task_ids.push(next_task.id.clone());
        } else {
            affected_task_ids.push(next_task.id.clone());
        }

        replacements.push(TaskReplacement {
            plan_id: plan_id.to_string(),
            from_revision,
            to_revision,
            task_id: next_task.id.clone(),
            change_kind,
            changed_fields,
            depends_on: next_task.depends_on.clone(),
            state: initial_state,
            admission_allowed: false,
            preserve_old_artifacts: existed_in_prev,
            live_steer_delivered: false,
        });
    }

    // Next include removed tasks from `previous` in declaration order.
    for prev_task in prev_views {
        if !next_by_id.contains_key(prev_task.id.as_str()) {
            let initial_state =
                initial_replacement_state(observed_prior_states.get(&prev_task.id).copied());
            affected_task_ids.push(prev_task.id.clone());
            replacements.push(TaskReplacement {
                plan_id: plan_id.to_string(),
                from_revision,
                to_revision,
                task_id: prev_task.id.clone(),
                change_kind: TaskChangeKind::Removed,
                changed_fields: vec!["removed".to_string()],
                depends_on: Vec::new(),
                state: initial_state,
                admission_allowed: false,
                preserve_old_artifacts: true,
                live_steer_delivered: false,
            });
        }
    }

    let mut report = PlanReconciliationReport {
        plan_id: plan_id.to_string(),
        from_revision,
        to_revision,
        affected_task_ids,
        unchanged_task_ids,
        replacements,
        live_steer_delivered: false,
    };

    recompute_admission_eligibility(&mut report);
    Ok(report)
}

fn initial_replacement_state(status: Option<PriorTaskStopStatus>) -> ReplacementState {
    match status.unwrap_or(PriorTaskStopStatus::Running) {
        PriorTaskStopStatus::NotStarted | PriorTaskStopStatus::ProvenStopped => {
            ReplacementState::Stopped
        }
        PriorTaskStopStatus::Running => ReplacementState::Pending,
        PriorTaskStopStatus::CancelRequested => ReplacementState::CancelRequested,
        PriorTaskStopStatus::Unknown => ReplacementState::Unknown,
    }
}

fn recompute_admission_eligibility(report: &mut PlanReconciliationReport) {
    let states_by_id: BTreeMap<String, ReplacementState> = report
        .replacements
        .iter()
        .map(|entry| (entry.task_id.clone(), entry.state))
        .collect();

    for entry in &mut report.replacements {
        entry.live_steer_delivered = false;
        if entry.change_kind == TaskChangeKind::Removed {
            entry.admission_allowed = false;
            continue;
        }
        if entry.state != ReplacementState::Stopped {
            entry.admission_allowed = false;
            continue;
        }
        let all_deps_admitted = entry.depends_on.iter().all(|dep_id| {
            states_by_id
                .get(dep_id)
                .is_some_and(|&dep_state| dep_state == ReplacementState::Admitted)
        });
        entry.admission_allowed = all_deps_admitted;
    }
}

/// Records that cancellation has been requested for an affected task (`Pending -> CancelRequested`).
///
/// Repeating `request_task_cancel` when already in `CancelRequested` is an idempotent no-op.
pub fn request_task_cancel(
    report: &PlanReconciliationReport,
    task_id: &str,
) -> Result<PlanReconciliationReport, PlanError> {
    let mut updated = report.clone();
    let Some(entry) = updated
        .replacements
        .iter_mut()
        .find(|entry| entry.task_id == task_id)
    else {
        return Err(PlanError::validation(
            "task_id",
            format!("task `{task_id}` not found in reconciliation report"),
        ));
    };

    match entry.state {
        ReplacementState::Pending => {
            entry.state = ReplacementState::CancelRequested;
        }
        ReplacementState::CancelRequested => {
            // Idempotent repeat.
        }
        other => {
            return Err(PlanError::InvalidReplacementTransition {
                plan_id: updated.plan_id.clone(),
                task_id: task_id.to_string(),
                current_state: other,
                reason: format!("cannot request cancellation from state `{other}`"),
            });
        }
    }

    recompute_admission_eligibility(&mut updated);
    Ok(updated)
}

/// Records that the prior task execution is now uncertain (`Pending | CancelRequested -> Unknown`).
///
/// Repeating `record_task_unknown_stop` when already in `Unknown` is an idempotent no-op.
/// Once in `Unknown`, replacement admission is blocked until [`record_task_proven_stop`] is called.
pub fn record_task_unknown_stop(
    report: &PlanReconciliationReport,
    task_id: &str,
) -> Result<PlanReconciliationReport, PlanError> {
    let mut updated = report.clone();
    let Some(entry) = updated
        .replacements
        .iter_mut()
        .find(|entry| entry.task_id == task_id)
    else {
        return Err(PlanError::validation(
            "task_id",
            format!("task `{task_id}` not found in reconciliation report"),
        ));
    };

    match entry.state {
        ReplacementState::Pending | ReplacementState::CancelRequested => {
            entry.state = ReplacementState::Unknown;
        }
        ReplacementState::Unknown => {
            // Idempotent repeat.
        }
        other => {
            return Err(PlanError::InvalidReplacementTransition {
                plan_id: updated.plan_id.clone(),
                task_id: task_id.to_string(),
                current_state: other,
                reason: format!("cannot transition from `{other}` to `unknown`"),
            });
        }
    }

    recompute_admission_eligibility(&mut updated);
    Ok(updated)
}

/// Records that the prior task execution has been proven stopped
/// (`Pending | CancelRequested | Unknown -> Stopped`).
///
/// Repeating `record_task_proven_stop` when already in `Stopped` is an idempotent no-op.
pub fn record_task_proven_stop(
    report: &PlanReconciliationReport,
    task_id: &str,
) -> Result<PlanReconciliationReport, PlanError> {
    let mut updated = report.clone();
    let Some(entry) = updated
        .replacements
        .iter_mut()
        .find(|entry| entry.task_id == task_id)
    else {
        return Err(PlanError::validation(
            "task_id",
            format!("task `{task_id}` not found in reconciliation report"),
        ));
    };

    match entry.state {
        ReplacementState::Pending
        | ReplacementState::CancelRequested
        | ReplacementState::Unknown => {
            entry.state = ReplacementState::Stopped;
        }
        ReplacementState::Stopped => {
            // Idempotent repeat.
        }
        ReplacementState::Admitted => {
            return Err(PlanError::InvalidReplacementTransition {
                plan_id: updated.plan_id.clone(),
                task_id: task_id.to_string(),
                current_state: ReplacementState::Admitted,
                reason: "cannot revert an already admitted replacement to `stopped`".to_string(),
            });
        }
    }

    recompute_admission_eligibility(&mut updated);
    Ok(updated)
}

/// Admits the replacement task for execution (`Stopped -> Admitted`).
///
/// - Repeating `admit_task_replacement` when already in `Admitted` is an idempotent no-op.
/// - Fails with [`PlanError::UnknownStopBlocksAdmission`] if the task (or any of its upstream
///   dependencies) is in [`ReplacementState::Unknown`].
/// - Fails with [`PlanError::ReplacementNotStopped`] if the task (or any of its upstream
///   dependencies) is still in `Pending`, `CancelRequested`, or un-admitted `Stopped`.
/// - Fails with [`PlanError::InvalidReplacementTransition`] if the task was removed in `to_revision`.
pub fn admit_task_replacement(
    report: &PlanReconciliationReport,
    task_id: &str,
) -> Result<PlanReconciliationReport, PlanError> {
    let states_by_id: BTreeMap<&str, ReplacementState> = report
        .replacements
        .iter()
        .map(|entry| (entry.task_id.as_str(), entry.state))
        .collect();

    let Some(target) = report
        .replacements
        .iter()
        .find(|entry| entry.task_id == task_id)
    else {
        return Err(PlanError::validation(
            "task_id",
            format!("task `{task_id}` not found in reconciliation report"),
        ));
    };

    if target.change_kind == TaskChangeKind::Removed {
        return Err(PlanError::InvalidReplacementTransition {
            plan_id: report.plan_id.clone(),
            task_id: task_id.to_string(),
            current_state: target.state,
            reason: "removed tasks have no replacement in the target revision".to_string(),
        });
    }

    if target.state == ReplacementState::Admitted {
        return Ok(report.clone());
    }

    if target.state == ReplacementState::Unknown {
        return Err(PlanError::UnknownStopBlocksAdmission {
            plan_id: report.plan_id.clone(),
            task_id: task_id.to_string(),
        });
    }

    if target.state != ReplacementState::Stopped {
        return Err(PlanError::ReplacementNotStopped {
            plan_id: report.plan_id.clone(),
            task_id: task_id.to_string(),
            current_state: target.state,
        });
    }

    for dep_id in &target.depends_on {
        if let Some(&dep_state) = states_by_id.get(dep_id.as_str()) {
            if dep_state == ReplacementState::Unknown {
                return Err(PlanError::UnknownStopBlocksAdmission {
                    plan_id: report.plan_id.clone(),
                    task_id: dep_id.clone(),
                });
            }
            if dep_state != ReplacementState::Admitted {
                return Err(PlanError::ReplacementNotStopped {
                    plan_id: report.plan_id.clone(),
                    task_id: dep_id.clone(),
                    current_state: dep_state,
                });
            }
        }
    }

    let mut updated = report.clone();
    if let Some(entry) = updated
        .replacements
        .iter_mut()
        .find(|entry| entry.task_id == task_id)
    {
        entry.state = ReplacementState::Admitted;
    }

    recompute_admission_eligibility(&mut updated);
    Ok(updated)
}

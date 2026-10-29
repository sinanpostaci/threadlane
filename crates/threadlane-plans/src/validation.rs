use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use crate::error::PlanError;
use crate::types::{PlanRevision, PlanTask};

/// Current on-disk schema version for `plans.json`.
pub const PLAN_STORE_SCHEMA_VERSION: u32 = 1;

/// Maximum JavaScript-safe integer revision (`2^53 - 1 = 9_007_199_254_740_991`).
pub const MAX_JS_SAFE_REVISION: u64 = 9_007_199_254_740_991;

/// Maximum byte length for `plan_id`, `binding_id`, `profile_id`, and `task.id`.
pub const MAX_ID_LEN: usize = 128;

/// Maximum byte length for a `PlanRevision::summary`.
pub const MAX_PLAN_SUMMARY_BYTES: usize = 4_096;

/// Maximum byte length for a `PlanTask::title`.
pub const MAX_TASK_TITLE_BYTES: usize = 256;

/// Maximum byte length for a `PlanTask::instruction` (64 KiB).
pub const MAX_TASK_INSTRUCTION_BYTES: usize = 65_536;

/// Maximum number of tasks permitted in a single [`PlanRevision`].
pub const MAX_TASKS_PER_PLAN: usize = 64;

/// Maximum number of upstream dependencies (`depends_on`) permitted on a single [`PlanTask`].
pub const MAX_DEPENDENCIES_PER_TASK: usize = 64;

/// Maximum number of `allowed_write_paths` entries permitted on a single [`PlanTask`].
pub const MAX_WRITE_PATHS_PER_TASK: usize = 64;

/// Maximum byte length for a single repository-relative write path entry.
pub const MAX_WRITE_PATH_BYTES: usize = 512;

/// Maximum number of distinct plans permitted in a single [`crate::PlanStore`].
pub const MAX_PLANS_IN_STORE: usize = 256;

/// Maximum number of revisions retained per plan in a single [`crate::PlanStore`].
pub const MAX_REVISIONS_PER_PLAN: usize = 256;

/// Maximum byte size (1 MiB) permitted when reading `plans.json` from disk.
pub const MAX_PLAN_STORE_BYTES: usize = 1_048_576;

/// Validates that `revision` does not exceed [`MAX_JS_SAFE_REVISION`].
pub fn validate_js_safe_revision(field: &'static str, revision: u64) -> Result<u64, PlanError> {
    if revision > MAX_JS_SAFE_REVISION {
        return Err(PlanError::validation(
            field,
            format!("{field} ({revision}) exceeds JS-safe limit of {MAX_JS_SAFE_REVISION}"),
        ));
    }
    Ok(revision)
}

/// Validates a non-empty bounded identifier (`plan_id`, `binding_id`, `profile_id`, `task.id`).
pub fn validate_identifier(field: &'static str, raw: &str) -> Result<String, PlanError> {
    if raw.is_empty() {
        return Err(PlanError::validation(
            field,
            format!("{field} must not be empty"),
        ));
    }
    if raw.len() > MAX_ID_LEN {
        return Err(PlanError::validation(
            field,
            format!("{field} exceeds maximum length of {MAX_ID_LEN} bytes"),
        ));
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(PlanError::validation(
            field,
            format!("{field} must not contain control characters or surrounding whitespace"),
        ));
    }
    if raw.starts_with('-') || raw.starts_with('.') {
        return Err(PlanError::validation(
            field,
            format!("{field} `{raw}` must not start with `-` or `.`"),
        ));
    }
    if raw.contains('/') || raw.contains('\\') || raw.contains("..") {
        return Err(PlanError::validation(
            field,
            format!("{field} `{raw}` must not contain path separators or traversal segments"),
        ));
    }
    Ok(raw.to_string())
}

/// Validates a plan summary string.
pub fn validate_summary(raw: &str) -> Result<String, PlanError> {
    if raw.len() > MAX_PLAN_SUMMARY_BYTES {
        return Err(PlanError::validation(
            "summary",
            format!("summary exceeds maximum length of {MAX_PLAN_SUMMARY_BYTES} bytes"),
        ));
    }
    if raw.contains('\0')
        || raw
            .chars()
            .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
    {
        return Err(PlanError::validation(
            "summary",
            "summary must not contain NUL or control characters",
        ));
    }
    if raw.trim().is_empty() {
        return Err(PlanError::validation(
            "summary",
            "summary must not be empty",
        ));
    }
    Ok(raw.to_string())
}

/// Validates a task title string.
pub fn validate_task_title(raw: &str) -> Result<String, PlanError> {
    if raw.len() > MAX_TASK_TITLE_BYTES {
        return Err(PlanError::validation(
            "title",
            format!("task title exceeds maximum length of {MAX_TASK_TITLE_BYTES} bytes"),
        ));
    }
    if raw.chars().any(char::is_control) {
        return Err(PlanError::validation(
            "title",
            "task title must not contain control characters",
        ));
    }
    if raw.trim().is_empty() {
        return Err(PlanError::validation(
            "title",
            "task title must not be empty",
        ));
    }
    Ok(raw.to_string())
}

/// Validates an untrusted task instruction string.
pub fn validate_task_instruction(raw: &str) -> Result<String, PlanError> {
    if raw.len() > MAX_TASK_INSTRUCTION_BYTES {
        return Err(PlanError::validation(
            "instruction",
            format!(
                "task instruction exceeds maximum length of {MAX_TASK_INSTRUCTION_BYTES} bytes"
            ),
        ));
    }
    if raw.contains('\0') {
        return Err(PlanError::validation(
            "instruction",
            "task instruction must not contain NUL bytes",
        ));
    }
    if raw.trim().is_empty() {
        return Err(PlanError::validation(
            "instruction",
            "task instruction must not be empty",
        ));
    }
    Ok(raw.to_string())
}

/// Returns `true` if `rel_path` targets Git metadata, repository control files, or agent/toolchain configuration.
pub fn is_protected_control_path(rel_path: &str) -> bool {
    let trimmed = rel_path.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }

    let path = Path::new(trimmed);
    let components: Vec<String> = path
        .components()
        .map(|comp| comp.as_os_str().to_string_lossy().to_ascii_lowercase())
        .collect();

    if components.is_empty() {
        return true;
    }

    for comp in &components {
        if matches!(
            comp.as_str(),
            ".git"
                | ".gitmodules"
                | ".gitattributes"
                | ".gitignore"
                | "agents.md"
                | "claude.md"
                | ".codex"
                | ".gemini"
                | ".opencode"
                | ".agents"
                | ".claude"
        ) {
            return true;
        }
        if comp == ".env" || comp.starts_with(".env.") {
            return true;
        }
    }

    let first = components[0].as_str();
    if first == ".github" {
        return true;
    }
    if first == ".cargo"
        && components
            .get(1)
            .is_some_and(|second| second == "config" || second == "config.toml")
    {
        return true;
    }

    false
}

/// Validates that `rel_path` is a clean, repository-relative, non-wildcard, non-protected write scope entry.
/// Exact file paths have no trailing slash; directory scopes end with a single trailing `/`.
pub fn validate_allowed_write_path(
    plan_id: &str,
    task_id: &str,
    rel_path: &str,
) -> Result<String, PlanError> {
    if rel_path.is_empty() || rel_path.len() > MAX_WRITE_PATH_BYTES {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: format!("write path must be between 1 and {MAX_WRITE_PATH_BYTES} bytes"),
        });
    }
    if rel_path.trim() != rel_path {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path must not have leading or trailing whitespace".to_string(),
        });
    }
    let stripped = rel_path.strip_suffix('/').unwrap_or(rel_path);
    if stripped.is_empty()
        || rel_path.starts_with('/')
        || stripped.ends_with('/')
        || rel_path.contains('\\')
        || rel_path.contains('\0')
        || rel_path.contains("//")
    {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason:
                "write path must be a normalized repository-relative file or directory (`dir/`) path without leading slashes, double slashes, or backslashes"
                    .to_string(),
        });
    }
    if rel_path
        .chars()
        .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}' | '!'))
    {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path must not contain wildcard or glob pattern characters".to_string(),
        });
    }
    if stripped
        .split('/')
        .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path must not contain `.` or `..` traversal components".to_string(),
        });
    }

    let path = Path::new(stripped);
    if path.is_absolute() {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path must be relative, not absolute".to_string(),
        });
    }

    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::Normal(part) => {
                let s = part.to_string_lossy();
                if s.is_empty() || s.chars().any(char::is_control) {
                    return Err(PlanError::InvalidWriteScope {
                        plan_id: plan_id.to_string(),
                        task_id: task_id.to_string(),
                        path: rel_path.to_string(),
                        reason: "write path must not contain control characters".to_string(),
                    });
                }
                saw_normal = true;
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(PlanError::InvalidWriteScope {
                    plan_id: plan_id.to_string(),
                    task_id: task_id.to_string(),
                    path: rel_path.to_string(),
                    reason: "write path must not contain `.` or `..` traversal components"
                        .to_string(),
                });
            }
        }
    }

    if !saw_normal {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path must contain at least one normal path component".to_string(),
        });
    }

    if is_protected_control_path(stripped) {
        return Err(PlanError::InvalidWriteScope {
            plan_id: plan_id.to_string(),
            task_id: task_id.to_string(),
            path: rel_path.to_string(),
            reason: "write path targets protected repository control or agent configuration files"
                .to_string(),
        });
    }

    Ok(rel_path.to_string())
}

/// Validates all tasks in a plan (count bounds, unique IDs, text bounds, write scopes,
/// dependency references, and DAG acyclicity) and returns the deterministic topological order
/// of task identifiers.
pub fn validate_plan_tasks(plan_id: &str, tasks: &[PlanTask]) -> Result<Vec<String>, PlanError> {
    if tasks.is_empty() || tasks.len() > MAX_TASKS_PER_PLAN {
        return Err(PlanError::validation(
            "tasks",
            format!(
                "plan must contain between 1 and {MAX_TASKS_PER_PLAN} tasks (got {})",
                tasks.len()
            ),
        ));
    }

    let mut index_by_id: BTreeMap<&str, usize> = BTreeMap::new();
    for (idx, task) in tasks.iter().enumerate() {
        validate_identifier("task.id", &task.id)?;
        validate_task_title(&task.title)?;
        validate_task_instruction(&task.instruction)?;

        if index_by_id.insert(task.id.as_str(), idx).is_some() {
            return Err(PlanError::DuplicateTaskId {
                plan_id: plan_id.to_string(),
                task_id: task.id.clone(),
            });
        }

        if task.allowed_write_paths.is_empty()
            || task.allowed_write_paths.len() > MAX_WRITE_PATHS_PER_TASK
        {
            return Err(PlanError::InvalidWriteScope {
                plan_id: plan_id.to_string(),
                task_id: task.id.clone(),
                path: String::new(),
                reason: format!(
                    "allowed_write_paths must contain between 1 and {MAX_WRITE_PATHS_PER_TASK} entries (got {})",
                    task.allowed_write_paths.len()
                ),
            });
        }

        let mut seen_paths = BTreeSet::new();
        for path in &task.allowed_write_paths {
            validate_allowed_write_path(plan_id, &task.id, path)?;
            if !seen_paths.insert(path.as_str()) {
                return Err(PlanError::InvalidWriteScope {
                    plan_id: plan_id.to_string(),
                    task_id: task.id.clone(),
                    path: path.clone(),
                    reason: format!("duplicate write path `{path}` on task `{}`", task.id),
                });
            }
        }

        if task.depends_on.len() > MAX_DEPENDENCIES_PER_TASK {
            return Err(PlanError::validation(
                "depends_on",
                format!(
                    "task `{}` exceeds maximum of {MAX_DEPENDENCIES_PER_TASK} dependencies",
                    task.id
                ),
            ));
        }
    }

    let n = tasks.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (idx, task) in tasks.iter().enumerate() {
        let mut seen_deps = BTreeSet::new();
        for dep_id in &task.depends_on {
            validate_identifier("depends_on", dep_id)?;
            if !seen_deps.insert(dep_id.as_str()) {
                return Err(PlanError::validation(
                    "depends_on",
                    format!(
                        "task `{}` contains duplicate dependency `{dep_id}`",
                        task.id
                    ),
                ));
            }
            if dep_id == &task.id {
                return Err(PlanError::DependencyCycle {
                    plan_id: plan_id.to_string(),
                    task_id: task.id.clone(),
                });
            }
            let Some(&dep_idx) = index_by_id.get(dep_id.as_str()) else {
                return Err(PlanError::UnknownDependency {
                    plan_id: plan_id.to_string(),
                    task_id: task.id.clone(),
                    dependency_id: dep_id.clone(),
                });
            };
            in_degree[idx] += 1;
            dependents[dep_idx].push(idx);
        }
    }

    // Deterministic Kahn topological sort preserving task declaration order among ready nodes.
    let mut ready: BTreeSet<usize> = in_degree
        .iter()
        .enumerate()
        .filter_map(|(idx, &deg)| (deg == 0).then_some(idx))
        .collect();
    let mut topological_order = Vec::with_capacity(n);

    while let Some(curr) = ready.pop_first() {
        topological_order.push(tasks[curr].id.clone());
        for &next_idx in &dependents[curr] {
            in_degree[next_idx] -= 1;
            if in_degree[next_idx] == 0 {
                ready.insert(next_idx);
            }
        }
    }

    if topological_order.len() != n {
        let cycle_task_id =
            find_cycle_task(tasks, &index_by_id).unwrap_or_else(|| tasks[0].id.clone());
        return Err(PlanError::DependencyCycle {
            plan_id: plan_id.to_string(),
            task_id: cycle_task_id,
        });
    }

    Ok(topological_order)
}

fn find_cycle_task(tasks: &[PlanTask], index_by_id: &BTreeMap<&str, usize>) -> Option<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum VisitState {
        Unvisited,
        Visiting,
        Visited,
    }

    fn dfs(
        node: usize,
        tasks: &[PlanTask],
        index_by_id: &BTreeMap<&str, usize>,
        state: &mut [VisitState],
    ) -> Option<String> {
        state[node] = VisitState::Visiting;
        for dep_id in &tasks[node].depends_on {
            if let Some(&dep_idx) = index_by_id.get(dep_id.as_str()) {
                match state[dep_idx] {
                    VisitState::Visiting => return Some(tasks[dep_idx].id.clone()),
                    VisitState::Unvisited => {
                        if let Some(found) = dfs(dep_idx, tasks, index_by_id, state) {
                            return Some(found);
                        }
                    }
                    VisitState::Visited => {}
                }
            }
        }
        state[node] = VisitState::Visited;
        None
    }

    let mut state = vec![VisitState::Unvisited; tasks.len()];
    for idx in 0..tasks.len() {
        if state[idx] == VisitState::Unvisited {
            if let Some(cycle_id) = dfs(idx, tasks, index_by_id, &mut state) {
                return Some(cycle_id);
            }
        }
    }
    None
}

/// Validates a complete [`PlanRevision`] structural record.
pub fn validate_plan_revision(revision: &PlanRevision) -> Result<Vec<String>, PlanError> {
    validate_identifier("plan_id", &revision.plan_id)?;
    validate_identifier("binding_id", &revision.binding_id)?;
    validate_identifier("profile_id", &revision.profile_id)?;
    if revision.revision == 0 {
        return Err(PlanError::validation(
            "revision",
            "persisted plan revision must be >= 1",
        ));
    }
    validate_js_safe_revision("revision", revision.revision)?;
    validate_js_safe_revision("profile_revision", revision.profile_revision)?;
    if revision.created_at_ms < 0 {
        return Err(PlanError::validation(
            "created_at_ms",
            "created_at_ms must be non-negative",
        ));
    }
    validate_summary(&revision.summary)?;
    validate_plan_tasks(&revision.plan_id, &revision.tasks)
}

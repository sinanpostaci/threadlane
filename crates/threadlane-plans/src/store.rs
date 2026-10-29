use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::PlanError;
use crate::types::{DeliveryReceipt, PlanChange, PlanRevision, PlanStatus};
use crate::validation::{
    validate_identifier, validate_js_safe_revision, validate_plan_revision, validate_plan_tasks,
    validate_summary, MAX_PLANS_IN_STORE, MAX_PLAN_STORE_BYTES, MAX_REVISIONS_PER_PLAN,
    PLAN_STORE_SCHEMA_VERSION,
};

const LEDGER_FILENAME: &str = "plans.json";
const LOCK_FILENAME: &str = "plans.lock";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPlanLedger {
    schema_version: u32,
    revisions: Vec<PlanRevision>,
}

impl Default for PersistedPlanLedger {
    fn default() -> Self {
        Self {
            schema_version: PLAN_STORE_SCHEMA_VERSION,
            revisions: Vec::new(),
        }
    }
}

/// Durable, cross-process locked, versioned plan ledger backed by a private directory (`0700`/`0600`).
#[derive(Debug)]
pub struct PlanStore {
    dir: PathBuf,
    ledger_path: PathBuf,
    lock_path: PathBuf,
    in_process_lock: Mutex<()>,
}

struct ExclusiveLockGuard {
    file: File,
}

impl Drop for ExclusiveLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl PlanStore {
    /// Opens or initializes a [`PlanStore`] in `dir`, enforcing private `0700`/`0600` permissions,
    /// symlink refusal, bounded reads, and structural ledger verification.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, PlanError> {
        ensure_platform_supported()?;
        let dir_path = ensure_private_dir(dir.as_ref(), "plan store directory")?;
        let ledger_path = dir_path.join(LEDGER_FILENAME);
        let lock_path = dir_path.join(LOCK_FILENAME);

        let store = Self {
            dir: dir_path,
            ledger_path,
            lock_path,
            in_process_lock: Mutex::new(()),
        };

        // Acquire cross-process lock on open and verify any pre-existing ledger file without
        // overwriting corrupt state.
        {
            let _in_proc = store
                .in_process_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _file_lock = store.acquire_exclusive_lock()?;
            let _ = store.read_ledger_locked()?;
        }

        Ok(store)
    }

    /// Returns the canonical private directory backing this [`PlanStore`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns a snapshot of all persisted [`PlanRevision`] records in deterministic
    /// `(plan_id, revision)` ascending order.
    pub fn snapshot(&self) -> Result<Vec<PlanRevision>, PlanError> {
        let _in_proc = self
            .in_process_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock = self.acquire_exclusive_lock()?;
        let ledger = self.read_ledger_locked()?;
        Ok(ledger.revisions)
    }

    /// Returns all revisions for `plan_id` in ascending revision order (`1..=current_revision`).
    pub fn plan_revisions(&self, plan_id: &str) -> Result<Vec<PlanRevision>, PlanError> {
        validate_identifier("plan_id", plan_id)?;
        let all = self.snapshot()?;
        Ok(all
            .into_iter()
            .filter(|rev| rev.plan_id == plan_id)
            .collect())
    }

    /// Returns the latest (`current`) [`PlanRevision`] for `plan_id`, or `None` if `plan_id`
    /// does not exist in the ledger.
    pub fn latest_revision(&self, plan_id: &str) -> Result<Option<PlanRevision>, PlanError> {
        let revisions = self.plan_revisions(plan_id)?;
        Ok(revisions.into_iter().last())
    }

    /// Returns a specific `(plan_id, revision)` from the ledger if present.
    pub fn get_revision(
        &self,
        plan_id: &str,
        revision: u64,
    ) -> Result<Option<PlanRevision>, PlanError> {
        validate_identifier("plan_id", plan_id)?;
        validate_js_safe_revision("revision", revision)?;
        let revisions = self.plan_revisions(plan_id)?;
        Ok(revisions.into_iter().find(|rev| rev.revision == revision))
    }

    /// Returns the [`DeliveryReceipt`] for a specific `(plan_id, revision)` if present.
    pub fn delivery_receipt(
        &self,
        plan_id: &str,
        revision: u64,
    ) -> Result<Option<DeliveryReceipt>, PlanError> {
        Ok(self
            .get_revision(plan_id, revision)?
            .map(|rev| rev.delivery_receipt()))
    }

    /// Returns all [`DeliveryReceipt`] records for `plan_id` in ascending revision order.
    pub fn delivery_receipts(&self, plan_id: &str) -> Result<Vec<DeliveryReceipt>, PlanError> {
        let revisions = self.plan_revisions(plan_id)?;
        Ok(revisions
            .iter()
            .map(PlanRevision::delivery_receipt)
            .collect())
    }

    /// Applies an atomic compare-and-swap [`PlanChange`] against `expected_revision` (the current
    /// revision of `change.plan_id()`, or `0` when creating a new plan).
    ///
    /// - [`PlanChange::SaveDraft`] validates the DAG/scopes/bounds, increments the revision number
    ///   (`expected_revision + 1`), marks any prior unpublished `Draft` revision of `plan_id` as
    ///   [`PlanStatus::Superseded`], and leaves any prior published revision untouched until the
    ///   new draft is explicitly published.
    /// - [`PlanChange::Publish`], [`PlanChange::Receive`], and [`PlanChange::Apply`] transition the
    ///   current revision (`revision == expected_revision == current_revision`) in place, treat an
    ///   exact repeat of the same transition as an idempotent no-op, and reject out-of-order or
    ///   backwards mutations on published revisions.
    pub fn change(
        &self,
        expected_revision: u64,
        change: PlanChange,
    ) -> Result<PlanRevision, PlanError> {
        self.change_at(expected_revision, change, current_time_ms())
    }

    /// Applies [`PlanStore::change`] using an explicit `now_ms` timestamp for newly created
    /// draft revisions.
    pub fn change_at(
        &self,
        expected_revision: u64,
        change: PlanChange,
        now_ms: i64,
    ) -> Result<PlanRevision, PlanError> {
        validate_js_safe_revision("expected_revision", expected_revision)?;
        if now_ms < 0 {
            return Err(PlanError::validation(
                "created_at_ms",
                "timestamp must be non-negative",
            ));
        }

        let _in_proc = self
            .in_process_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock = self.acquire_exclusive_lock()?;
        let mut ledger = self.read_ledger_locked()?;

        let updated = apply_change_to_ledger(&mut ledger, expected_revision, change, now_ms)?;
        self.write_ledger_locked(&ledger)?;
        Ok(updated)
    }

    fn acquire_exclusive_lock(&self) -> Result<ExclusiveLockGuard, PlanError> {
        ensure_private_dir(&self.dir, "plan store directory")?;
        let file = open_private_lock_file(&self.lock_path, "plan store lock file")?;
        file.lock_exclusive().map_err(|err| {
            PlanError::Io(format!(
                "failed to lock {}: {err}",
                self.lock_path.display()
            ))
        })?;
        Ok(ExclusiveLockGuard { file })
    }

    fn read_ledger_locked(&self) -> Result<PersistedPlanLedger, PlanError> {
        validate_preexisting_private_file(&self.ledger_path, "plan store ledger file")?;

        let file = match File::open(&self.ledger_path) {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PersistedPlanLedger::default());
            }
            Err(err) => return Err(PlanError::Io(err.to_string())),
        };

        let meta = fs::symlink_metadata(&self.ledger_path)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err(PlanError::SecurityViolation(format!(
                "plan store ledger `{}` must be a regular non-symlink file",
                self.ledger_path.display()
            )));
        }
        if meta.len() > MAX_PLAN_STORE_BYTES as u64 {
            return Err(PlanError::SecurityViolation(format!(
                "plan store ledger `{}` size ({} bytes) exceeds maximum of {MAX_PLAN_STORE_BYTES} bytes",
                self.ledger_path.display(),
                meta.len()
            )));
        }

        let mut buf = Vec::new();
        file.take(MAX_PLAN_STORE_BYTES as u64 + 1)
            .read_to_end(&mut buf)?;
        if buf.len() > MAX_PLAN_STORE_BYTES {
            return Err(PlanError::SecurityViolation(format!(
                "plan store ledger `{}` exceeded maximum read bound of {MAX_PLAN_STORE_BYTES} bytes",
                self.ledger_path.display()
            )));
        }

        let mut ledger: PersistedPlanLedger = serde_json::from_slice(&buf).map_err(|err| {
            PlanError::CorruptStore(format!("invalid JSON in `{LEDGER_FILENAME}`: {err}"))
        })?;

        validate_and_normalize_ledger(&mut ledger)?;
        Ok(ledger)
    }

    fn write_ledger_locked(&self, ledger: &PersistedPlanLedger) -> Result<(), PlanError> {
        validate_preexisting_private_file(&self.ledger_path, "plan store ledger file")?;

        let serialized = serde_json::to_vec_pretty(ledger)?;
        if serialized.len() > MAX_PLAN_STORE_BYTES {
            return Err(PlanError::Validation {
                field: "plans".to_string(),
                reason: format!(
                    "serialized plan ledger ({} bytes) exceeds maximum of {MAX_PLAN_STORE_BYTES} bytes",
                    serialized.len()
                ),
            });
        }

        let temp_name = format!(
            ".{LEDGER_FILENAME}.tmp.{}.{}",
            std::process::id(),
            Uuid::new_v4()
        );
        let temp_path = self.dir.join(temp_name);
        let _ = validate_absolute_no_symlink_components(&temp_path, "temporary plan ledger file")?;

        let write_result = (|| -> Result<(), PlanError> {
            #[cfg(unix)]
            let mut temp_file = {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
                let f = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&temp_path)?;
                f.set_permissions(fs::Permissions::from_mode(0o600))?;
                f
            };

            #[cfg(not(unix))]
            let mut temp_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;

            temp_file.write_all(&serialized)?;
            temp_file.sync_all()?;
            drop(temp_file);

            fs::rename(&temp_path, &self.ledger_path)?;
            let dir_file = File::open(&self.dir)?;
            dir_file.sync_all()?;
            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }

        write_result
    }
}

fn apply_change_to_ledger(
    ledger: &mut PersistedPlanLedger,
    expected_revision: u64,
    change: PlanChange,
    now_ms: i64,
) -> Result<PlanRevision, PlanError> {
    match change {
        PlanChange::SaveDraft {
            plan_id,
            binding_id,
            profile_id,
            profile_revision,
            summary,
            tasks,
        } => {
            validate_identifier("plan_id", &plan_id)?;
            validate_identifier("binding_id", &binding_id)?;
            validate_identifier("profile_id", &profile_id)?;
            validate_js_safe_revision("profile_revision", profile_revision)?;
            validate_summary(&summary)?;
            validate_plan_tasks(&plan_id, &tasks)?;

            let mut existing_indices = Vec::new();
            let mut distinct_plans = std::collections::BTreeSet::new();
            for (idx, rev) in ledger.revisions.iter().enumerate() {
                distinct_plans.insert(rev.plan_id.as_str());
                if rev.plan_id == plan_id {
                    existing_indices.push(idx);
                }
            }

            let actual_revision = existing_indices
                .last()
                .map(|&idx| ledger.revisions[idx].revision)
                .unwrap_or(0);

            if expected_revision != actual_revision {
                return Err(PlanError::StaleRevision {
                    plan_id,
                    expected_revision,
                    actual_revision,
                });
            }

            if existing_indices.is_empty() && distinct_plans.len() >= MAX_PLANS_IN_STORE {
                return Err(PlanError::validation(
                    "plan_id",
                    format!("plan store cannot exceed {MAX_PLANS_IN_STORE} distinct plans"),
                ));
            }
            if existing_indices.len() >= MAX_REVISIONS_PER_PLAN {
                return Err(PlanError::validation(
                    "revision",
                    format!("plan `{plan_id}` cannot exceed {MAX_REVISIONS_PER_PLAN} revisions"),
                ));
            }

            let mut created_at_ms = now_ms;
            if let Some(&first_idx) = existing_indices.first() {
                let expected_binding = &ledger.revisions[first_idx].binding_id;
                if expected_binding != &binding_id {
                    return Err(PlanError::BindingMismatch {
                        plan_id,
                        expected_binding_id: expected_binding.clone(),
                        actual_binding_id: binding_id,
                    });
                }
            }
            if let Some(&last_idx) = existing_indices.last() {
                let prev_created = ledger.revisions[last_idx].created_at_ms;
                if created_at_ms < prev_created {
                    created_at_ms = prev_created;
                }
            }

            let new_revision = actual_revision
                .checked_add(1)
                .ok_or_else(|| PlanError::validation("revision", "revision counter overflow"))?;
            validate_js_safe_revision("revision", new_revision)?;

            // Supersede any earlier unpublished Draft revisions of this plan while leaving
            // published/received/applied revisions untouched until the new draft is published.
            for &idx in &existing_indices {
                if ledger.revisions[idx].status == PlanStatus::Draft {
                    ledger.revisions[idx].status = PlanStatus::Superseded;
                }
            }

            let draft = PlanRevision {
                plan_id,
                binding_id,
                revision: new_revision,
                profile_id,
                profile_revision,
                summary,
                tasks,
                status: PlanStatus::Draft,
                created_at_ms,
            };

            ledger.revisions.push(draft.clone());
            ledger
                .revisions
                .sort_by(|a, b| a.plan_id.cmp(&b.plan_id).then(a.revision.cmp(&b.revision)));

            Ok(draft)
        }
        PlanChange::Publish { plan_id, revision } => transition_existing_revision(
            ledger,
            &plan_id,
            expected_revision,
            revision,
            PlanStatus::Published,
        ),
        PlanChange::Receive { plan_id, revision } => transition_existing_revision(
            ledger,
            &plan_id,
            expected_revision,
            revision,
            PlanStatus::Received,
        ),
        PlanChange::Apply { plan_id, revision } => transition_existing_revision(
            ledger,
            &plan_id,
            expected_revision,
            revision,
            PlanStatus::Applied,
        ),
    }
}

fn transition_existing_revision(
    ledger: &mut PersistedPlanLedger,
    plan_id: &str,
    expected_revision: u64,
    target_revision: u64,
    target_status: PlanStatus,
) -> Result<PlanRevision, PlanError> {
    validate_identifier("plan_id", plan_id)?;
    validate_js_safe_revision("revision", target_revision)?;

    let mut plan_indices = Vec::new();
    for (idx, rev) in ledger.revisions.iter().enumerate() {
        if rev.plan_id == plan_id {
            plan_indices.push(idx);
        }
    }

    let Some(&last_idx) = plan_indices.last() else {
        if expected_revision != 0 {
            return Err(PlanError::StaleRevision {
                plan_id: plan_id.to_string(),
                expected_revision,
                actual_revision: 0,
            });
        }
        return Err(PlanError::PlanNotFound {
            plan_id: plan_id.to_string(),
        });
    };

    let actual_revision = ledger.revisions[last_idx].revision;
    if expected_revision != actual_revision {
        return Err(PlanError::StaleRevision {
            plan_id: plan_id.to_string(),
            expected_revision,
            actual_revision,
        });
    }
    if target_revision != actual_revision {
        // If caller targeted an older published/superseded revision while expected_revision matched
        // current_revision, report stale revision or immutable published revision.
        return Err(PlanError::StaleRevision {
            plan_id: plan_id.to_string(),
            expected_revision: target_revision,
            actual_revision,
        });
    }

    let current_status = ledger.revisions[last_idx].status;

    // Idempotent repeat of exact same transition on current revision.
    if current_status == target_status {
        return Ok(ledger.revisions[last_idx].clone());
    }

    match (current_status, target_status) {
        (PlanStatus::Draft, PlanStatus::Published) => {
            // Re-validate revision invariants before publishing and mark all prior revisions of
            // `plan_id` as Superseded without altering their payload fields.
            validate_plan_revision(&ledger.revisions[last_idx])?;
            for &idx in &plan_indices {
                if idx != last_idx && ledger.revisions[idx].status != PlanStatus::Superseded {
                    ledger.revisions[idx].status = PlanStatus::Superseded;
                }
            }
            ledger.revisions[last_idx].status = PlanStatus::Published;
            Ok(ledger.revisions[last_idx].clone())
        }
        (PlanStatus::Published, PlanStatus::Received) => {
            ledger.revisions[last_idx].status = PlanStatus::Received;
            Ok(ledger.revisions[last_idx].clone())
        }
        (PlanStatus::Received, PlanStatus::Applied) => {
            ledger.revisions[last_idx].status = PlanStatus::Applied;
            Ok(ledger.revisions[last_idx].clone())
        }
        (
            PlanStatus::Received | PlanStatus::Applied | PlanStatus::Superseded,
            PlanStatus::Published,
        ) => Err(PlanError::ImmutablePublishedRevision {
            plan_id: plan_id.to_string(),
            revision: actual_revision,
            status: current_status,
            attempted_action: "publish".to_string(),
        }),
        _ => Err(PlanError::InvalidStateTransition {
            plan_id: plan_id.to_string(),
            revision: actual_revision,
            current_status,
            attempted_status: target_status,
        }),
    }
}

fn validate_and_normalize_ledger(ledger: &mut PersistedPlanLedger) -> Result<(), PlanError> {
    if ledger.schema_version != PLAN_STORE_SCHEMA_VERSION {
        return Err(PlanError::CorruptStore(format!(
            "unsupported plan ledger schema version {} (expected {})",
            ledger.schema_version, PLAN_STORE_SCHEMA_VERSION
        )));
    }

    ledger
        .revisions
        .sort_by(|a, b| a.plan_id.cmp(&b.plan_id).then(a.revision.cmp(&b.revision)));

    let mut by_plan: BTreeMap<&str, Vec<&PlanRevision>> = BTreeMap::new();
    for rev in &ledger.revisions {
        validate_plan_revision(rev).map_err(|err| {
            PlanError::CorruptStore(format!(
                "invalid persisted plan `{}` revision {}: {err}",
                rev.plan_id, rev.revision
            ))
        })?;
        by_plan.entry(rev.plan_id.as_str()).or_default().push(rev);
    }

    if by_plan.len() > MAX_PLANS_IN_STORE {
        return Err(PlanError::CorruptStore(format!(
            "persisted plan count ({}) exceeds limit of {MAX_PLANS_IN_STORE}",
            by_plan.len()
        )));
    }

    for (plan_id, revs) in by_plan {
        if revs.len() > MAX_REVISIONS_PER_PLAN {
            return Err(PlanError::CorruptStore(format!(
                "plan `{plan_id}` has {} revisions (max {MAX_REVISIONS_PER_PLAN})",
                revs.len()
            )));
        }

        let expected_binding = &revs[0].binding_id;
        for (idx, rev) in revs.iter().enumerate() {
            let expected_rev_num = (idx as u64) + 1;
            if rev.revision != expected_rev_num {
                return Err(PlanError::CorruptStore(format!(
                    "plan `{plan_id}` has non-contiguous revision number {} at position {}",
                    rev.revision, expected_rev_num
                )));
            }
            if &rev.binding_id != expected_binding {
                return Err(PlanError::CorruptStore(format!(
                    "plan `{plan_id}` revision {} has inconsistent binding_id `{}` (expected `{}`)",
                    rev.revision, rev.binding_id, expected_binding
                )));
            }
        }
    }

    Ok(())
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| i64::try_from(dur.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn ensure_platform_supported() -> Result<(), PlanError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(PlanError::UnsupportedPlatform(
            "0700/0600 private plan ledger permissions are only implemented on Unix".to_string(),
        ))
    }
}

fn resolve_clean_path(path: &Path, label: &str) -> Result<PathBuf, PlanError> {
    if path.as_os_str().is_empty() {
        return Err(PlanError::SecurityViolation(format!(
            "{label} path must not be empty"
        )));
    }
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    validate_absolute_no_symlink_components(&abs, label)
}

fn validate_absolute_no_symlink_components(path: &Path, label: &str) -> Result<PathBuf, PlanError> {
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(PlanError::SecurityViolation(format!(
            "{label} `{}` must be a non-empty absolute path",
            path.display()
        )));
    }

    let mut current = PathBuf::new();
    let mut components = path.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(PlanError::SecurityViolation(format!(
                    "{label} `{}` must not contain `.` or `..` components",
                    path.display()
                )));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(PlanError::SecurityViolation(format!(
                                "{label} `{}` traverses symlink component `{}`",
                                path.display(),
                                current.display()
                            )));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(PlanError::SecurityViolation(format!(
                                "{label} parent component `{}` is not a directory",
                                current.display()
                            )));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(PlanError::Io(err.to_string())),
                }
            }
        }
    }

    Ok(current)
}

fn ensure_private_dir(path: &Path, label: &str) -> Result<PathBuf, PlanError> {
    let checked = resolve_clean_path(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut current = PathBuf::new();
        for comp in checked.components() {
            match comp {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir | Component::ParentDir => {
                    return Err(PlanError::SecurityViolation(format!(
                        "{label} `{}` must not contain relative traversal components",
                        path.display()
                    )));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(PlanError::SecurityViolation(format!(
                                    "{label} `{}` traverses symlink `{}`",
                                    path.display(),
                                    current.display()
                                )));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(PlanError::SecurityViolation(format!(
                                    "{label} component `{}` is not a directory",
                                    current.display()
                                )));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current)?;
                            fs::set_permissions(&current, fs::Permissions::from_mode(0o700))?;
                        }
                        Err(err) => return Err(PlanError::Io(err.to_string())),
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&checked)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(PlanError::SecurityViolation(format!(
                "{label} `{}` must be a non-symlink directory",
                checked.display()
            )));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(PlanError::SecurityViolation(format!(
                "{label} `{}` has non-private permissions {:04o} (expected 0700); refusing to chmod existing directory",
                checked.display(),
                mode
            )));
        }
    }

    Ok(checked)
}

fn validate_preexisting_private_file(path: &Path, label: &str) -> Result<(), PlanError> {
    let _ = validate_absolute_no_symlink_components(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(PlanError::SecurityViolation(format!(
                        "{label} `{}` must not be a symlink",
                        path.display()
                    )));
                }
                if !meta.file_type().is_file() {
                    return Err(PlanError::SecurityViolation(format!(
                        "{label} `{}` must be a regular file",
                        path.display()
                    )));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(PlanError::SecurityViolation(format!(
                        "{label} `{}` has insecure permissions {:04o} (expected 0600)",
                        path.display(),
                        mode
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(PlanError::Io(err.to_string())),
        }
    }

    Ok(())
}

fn open_private_lock_file(path: &Path, label: &str) -> Result<File, PlanError> {
    validate_preexisting_private_file(path, label)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;

        let sym_meta = fs::symlink_metadata(path)?;
        if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
            return Err(PlanError::SecurityViolation(format!(
                "{label} `{}` must be a regular non-symlink file",
                path.display()
            )));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        Err(PlanError::UnsupportedPlatform(
            "private lock files are only supported on Unix".to_string(),
        ))
    }
}

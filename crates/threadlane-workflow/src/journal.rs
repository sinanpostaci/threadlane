use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_plans::{
    validate_identifier, validate_js_safe_revision, validate_plan_revision, validate_summary,
    PlanRevision, PlanStatus, PlanTask,
};
use uuid::Uuid;

use crate::error::WorkflowError;
use crate::types::ScopedProfileDependencyProof;

/// Schema version for persisted per-binding command-key journal files.
pub const WORKFLOW_JOURNAL_SCHEMA_VERSION: u32 = 1;

/// Maximum number of command-key records permitted in a single binding's command journal.
pub const MAX_COMMAND_JOURNAL_ENTRIES: usize = 1_024;

/// Maximum byte size (1 MiB) permitted for a single binding's command journal file.
pub const MAX_COMMAND_JOURNAL_BYTES: usize = 1_048_576;

static IN_PROCESS_JOURNAL_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum CommandJournalState {
    Pending,
    Completed {
        created_revision: u64,
        created_at_ms: i64,
        plan_digest: Option<String>,
    },
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommandJournalEntry {
    pub command_key: String,
    pub explicit_plan_id: Option<String>,
    pub resolved_plan_id: String,
    pub expected_revision: u64,
    pub target_revision: u64,
    pub profile_id: String,
    pub profile_revision: u64,
    pub summary: String,
    pub tasks_digest: String,
    pub payload_digest: String,
    #[serde(default)]
    pub plan_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoped_profile_dependency_proof: Option<ScopedProfileDependencyProof>,
    pub state: CommandJournalState,
}

impl CommandJournalEntry {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn matches_intent(
        &self,
        target_plan_id: &str,
        expected_revision: u64,
        profile_id: &str,
        profile_revision: u64,
        summary: &str,
        tasks_digest: &str,
        payload_digest: &str,
        scoped_profile_dependency_proof: Option<&ScopedProfileDependencyProof>,
    ) -> bool {
        self.resolved_plan_id == target_plan_id
            && (expected_revision == self.expected_revision
                || expected_revision == self.target_revision)
            && self.profile_id == profile_id
            && self.profile_revision == profile_revision
            && self.summary == summary
            && self.tasks_digest == tasks_digest
            && self.payload_digest == payload_digest
            && self.scoped_profile_dependency_proof.as_ref() == scoped_profile_dependency_proof
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PersistedCommandJournal {
    pub schema_version: u32,
    pub binding_id: String,
    pub entries: Vec<CommandJournalEntry>,
}

impl PersistedCommandJournal {
    pub(crate) fn new(binding_id: &str) -> Self {
        Self {
            schema_version: WORKFLOW_JOURNAL_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            entries: Vec::new(),
        }
    }

    pub(crate) fn find(&self, command_key: &str) -> Option<&CommandJournalEntry> {
        self.entries.iter().find(|e| e.command_key == command_key)
    }

    pub(crate) fn find_mut(&mut self, command_key: &str) -> Option<&mut CommandJournalEntry> {
        self.entries
            .iter_mut()
            .find(|e| e.command_key == command_key)
    }

    pub(crate) fn verified_scoped_profile_dependency_proof(
        &self,
        binding_id: &str,
        revision: &PlanRevision,
    ) -> Option<&ScopedProfileDependencyProof> {
        if self.schema_version != WORKFLOW_JOURNAL_SCHEMA_VERSION
            || self.binding_id != binding_id
            || revision.binding_id != binding_id
            || revision.status == PlanStatus::Superseded
            || validate_plan_revision(revision).is_err()
            || revision.revision == 0
            || revision.profile_revision == 0
            || self.entries.len() > MAX_COMMAND_JOURNAL_ENTRIES
        {
            return None;
        }

        let expected_tasks_digest = compute_tasks_digest(&revision.tasks).ok()?;
        let mut matched_proof: Option<&ScopedProfileDependencyProof> = None;

        for entry in &self.entries {
            let touches_plan = entry.resolved_plan_id == revision.plan_id
                || entry
                    .explicit_plan_id
                    .as_deref()
                    .is_some_and(|id| id == revision.plan_id);
            if !touches_plan {
                continue;
            }

            let touches_revision = entry.target_revision == revision.revision
                || matches!(
                    entry.state,
                    CommandJournalState::Completed {
                        created_revision,
                        ..
                    } if created_revision == revision.revision
                );
            if !touches_revision {
                continue;
            }

            if entry.resolved_plan_id != revision.plan_id
                || entry
                    .explicit_plan_id
                    .as_deref()
                    .is_some_and(|id| id != revision.plan_id)
            {
                return None;
            }

            let expected_payload_digest = compute_payload_digest_with_proof(
                &revision.plan_id,
                &revision.profile_id,
                revision.profile_revision,
                &revision.summary,
                &revision.tasks,
                entry.scoped_profile_dependency_proof.as_ref(),
            )
            .ok()?;

            match &entry.state {
                CommandJournalState::Pending => {
                    return None;
                }
                CommandJournalState::Failed => {
                    if entry.target_revision == revision.revision
                        && entry.profile_id == revision.profile_id
                        && entry.profile_revision == revision.profile_revision
                        && entry.summary == revision.summary
                        && entry.tasks_digest == expected_tasks_digest
                        && entry.payload_digest == expected_payload_digest
                    {
                        return None;
                    }
                }
                CommandJournalState::Completed {
                    created_revision,
                    created_at_ms,
                    plan_digest,
                } => {
                    if matched_proof.is_some() {
                        return None;
                    }
                    if *created_revision != revision.revision
                        || entry.target_revision != revision.revision
                        || entry.expected_revision.checked_add(1) != Some(revision.revision)
                        || *created_at_ms < 0
                        || entry.profile_id != revision.profile_id
                        || entry.profile_revision != revision.profile_revision
                        || entry.summary != revision.summary
                        || entry.tasks_digest != expected_tasks_digest
                        || entry.payload_digest != expected_payload_digest
                    {
                        return None;
                    }
                    let (Some(entry_digest), Some(completed_digest)) =
                        (entry.plan_digest.as_deref(), plan_digest.as_deref())
                    else {
                        return None;
                    };
                    if entry_digest != completed_digest || !is_hex_64(completed_digest) {
                        return None;
                    }
                    let proof = entry.scoped_profile_dependency_proof.as_ref()?;
                    if proof.validate().is_err()
                        || proof.provenance_revision != revision.profile_revision
                        || proof.spec.profile_id != revision.profile_id
                    {
                        return None;
                    }
                    matched_proof = Some(proof);
                }
            }
        }

        matched_proof
    }

    pub(crate) fn verified_historical_plan_digest(
        &self,
        binding_id: &str,
        revision: &PlanRevision,
    ) -> Option<String> {
        if self.schema_version != WORKFLOW_JOURNAL_SCHEMA_VERSION
            || self.binding_id != binding_id
            || revision.binding_id != binding_id
            || !(revision.status.is_active_published() || revision.status == PlanStatus::Superseded)
            || validate_plan_revision(revision).is_err()
            || revision.revision == 0
            || revision.profile_revision == 0
            || self.entries.len() > MAX_COMMAND_JOURNAL_ENTRIES
        {
            return None;
        }

        let expected_tasks_digest = compute_tasks_digest(&revision.tasks).ok()?;

        let mut matched_digest: Option<String> = None;

        for entry in &self.entries {
            let touches_plan = entry.resolved_plan_id == revision.plan_id
                || entry
                    .explicit_plan_id
                    .as_deref()
                    .is_some_and(|id| id == revision.plan_id);
            if !touches_plan {
                continue;
            }

            let touches_revision = entry.target_revision == revision.revision
                || matches!(
                    entry.state,
                    CommandJournalState::Completed {
                        created_revision,
                        ..
                    } if created_revision == revision.revision
                );
            if !touches_revision {
                continue;
            }

            if entry.resolved_plan_id != revision.plan_id
                || entry
                    .explicit_plan_id
                    .as_deref()
                    .is_some_and(|id| id != revision.plan_id)
            {
                return None;
            }

            let expected_payload_digest = compute_payload_digest_with_proof(
                &revision.plan_id,
                &revision.profile_id,
                revision.profile_revision,
                &revision.summary,
                &revision.tasks,
                entry.scoped_profile_dependency_proof.as_ref(),
            )
            .ok()?;

            match &entry.state {
                CommandJournalState::Pending => {
                    return None;
                }
                CommandJournalState::Failed => {
                    if entry.target_revision == revision.revision
                        && entry.profile_id == revision.profile_id
                        && entry.profile_revision == revision.profile_revision
                        && entry.summary == revision.summary
                        && entry.tasks_digest == expected_tasks_digest
                        && entry.payload_digest == expected_payload_digest
                    {
                        return None;
                    }
                }
                CommandJournalState::Completed {
                    created_revision,
                    created_at_ms,
                    plan_digest,
                } => {
                    if matched_digest.is_some() {
                        return None;
                    }
                    if *created_revision != revision.revision
                        || entry.target_revision != revision.revision
                        || entry.expected_revision.checked_add(1) != Some(revision.revision)
                        || *created_at_ms < 0
                        || entry.profile_id != revision.profile_id
                        || entry.profile_revision != revision.profile_revision
                        || entry.summary != revision.summary
                        || entry.tasks_digest != expected_tasks_digest
                        || entry.payload_digest != expected_payload_digest
                    {
                        return None;
                    }
                    if let Some(ref proof) = entry.scoped_profile_dependency_proof {
                        if proof.validate().is_err()
                            || proof.provenance_revision != revision.profile_revision
                            || proof.spec.profile_id != revision.profile_id
                        {
                            return None;
                        }
                    }
                    let (Some(entry_digest), Some(completed_digest)) =
                        (entry.plan_digest.as_deref(), plan_digest.as_deref())
                    else {
                        return None;
                    };
                    if entry_digest != completed_digest || !is_hex_64(completed_digest) {
                        return None;
                    }
                    matched_digest = Some(completed_digest.to_string());
                }
            }
        }

        matched_digest
    }
}

struct ExclusiveFileLockGuard {
    file: File,
}

impl Drop for ExclusiveFileLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(crate) fn binding_journal_tag(binding_id: &str) -> Result<String, WorkflowError> {
    let seed = ("threadlane-workflow:command-journal:v1", binding_id);
    let Ok(encoded) = serde_json::to_vec(&seed) else {
        return Err(WorkflowError::Serialization(
            "Failed to encode command journal binding seed.",
        ));
    };
    let digest = Sha256::digest(&encoded);
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
}

pub(crate) fn journal_paths_for_binding(
    plans_dir: &Path,
    binding_id: &str,
) -> Result<(PathBuf, PathBuf), WorkflowError> {
    let tag = binding_journal_tag(binding_id)?;
    let journal_path = plans_dir.join(format!("workflow-commands-{tag}.json"));
    let lock_path = plans_dir.join(format!("workflow-commands-{tag}.lock"));
    Ok((journal_path, lock_path))
}

pub(crate) fn compute_tasks_digest(tasks: &[PlanTask]) -> Result<String, WorkflowError> {
    sha256_hex_64(&("threadlane-workflow:tasks:v1", tasks))
}

pub(crate) fn compute_payload_digest(
    resolved_plan_id: &str,
    profile_id: &str,
    profile_revision: u64,
    summary: &str,
    tasks: &[PlanTask],
) -> Result<String, WorkflowError> {
    let tuple = (
        "threadlane-workflow:draft-payload:v1",
        resolved_plan_id,
        profile_id,
        profile_revision,
        summary,
        tasks,
    );
    sha256_hex_64(&tuple)
}

pub(crate) fn compute_payload_digest_with_proof(
    resolved_plan_id: &str,
    profile_id: &str,
    profile_revision: u64,
    summary: &str,
    tasks: &[PlanTask],
    scoped_profile_dependency_proof: Option<&ScopedProfileDependencyProof>,
) -> Result<String, WorkflowError> {
    match scoped_profile_dependency_proof {
        None => compute_payload_digest(
            resolved_plan_id,
            profile_id,
            profile_revision,
            summary,
            tasks,
        ),
        Some(proof) => {
            let tuple = (
                "threadlane-workflow:draft-payload:v2",
                resolved_plan_id,
                profile_id,
                profile_revision,
                summary,
                tasks,
                proof,
            );
            sha256_hex_64(&tuple)
        }
    }
}

fn sha256_hex_64<T: Serialize + ?Sized>(value: &T) -> Result<String, WorkflowError> {
    let Ok(encoded) = serde_json::to_vec(value) else {
        return Err(WorkflowError::Serialization(
            "Failed to encode workflow command digest payload.",
        ));
    };
    let digest = Sha256::digest(&encoded);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut out, "{byte:02x}");
    }
    Ok(out)
}

pub(crate) fn with_journal_lock<T, F>(
    dir: &Path,
    lock_path: &Path,
    f: F,
) -> Result<T, WorkflowError>
where
    F: FnOnce(&Path) -> Result<T, WorkflowError>,
{
    ensure_platform_supported()?;
    let clean_dir = ensure_private_dir(dir)?;
    let clean_lock_path = validate_absolute_no_symlink_components(lock_path)?;
    if clean_lock_path.parent() != Some(clean_dir.as_path()) {
        return Err(WorkflowError::SecurityViolation(
            "Command journal lock file must reside directly inside the private plan store directory.",
        ));
    }

    let arc_mutex = {
        let map_mutex = IN_PROCESS_JOURNAL_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut map = map_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            map.entry(clean_lock_path.clone())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    };
    let _in_proc_guard = arc_mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let file = open_private_lock_file(&clean_lock_path)?;
    if file.lock_exclusive().is_err() {
        return Err(WorkflowError::Io(
            "Failed to acquire exclusive lock on workflow command journal.",
        ));
    }
    let _file_lock_guard = ExclusiveFileLockGuard { file };

    f(&clean_dir)
}

pub(crate) fn read_journal_locked(
    journal_path: &Path,
    binding_id: &str,
) -> Result<PersistedCommandJournal, WorkflowError> {
    validate_preexisting_private_file(journal_path)?;

    let file = match File::open(journal_path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PersistedCommandJournal::new(binding_id));
        }
        Err(_) => {
            return Err(WorkflowError::Io(
                "Failed to open workflow command journal.",
            ));
        }
    };

    let meta = fs::symlink_metadata(journal_path)
        .map_err(|_| WorkflowError::Io("Failed to read workflow command journal metadata."))?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal must be a regular non-symlink file.",
        ));
    }
    if meta.len() > MAX_COMMAND_JOURNAL_BYTES as u64 {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal file size exceeds the 1 MiB security limit.",
        ));
    }

    let mut buf = Vec::new();
    file.take(MAX_COMMAND_JOURNAL_BYTES as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(|_| WorkflowError::Io("Failed to read workflow command journal."))?;
    if buf.len() > MAX_COMMAND_JOURNAL_BYTES {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal exceeded the 1 MiB read bound.",
        ));
    }

    let Ok(journal) = serde_json::from_slice::<PersistedCommandJournal>(&buf) else {
        return Err(WorkflowError::CorruptStore(
            "Persisted workflow command journal is corrupt or incompatible.",
        ));
    };

    validate_journal(&journal, binding_id)?;
    Ok(journal)
}

pub(crate) fn read_journal_readonly(
    plans_dir: &Path,
    journal_path: &Path,
    binding_id: &str,
) -> Result<Option<PersistedCommandJournal>, WorkflowError> {
    ensure_platform_supported()?;
    let clean_dir = ensure_private_dir(plans_dir)?;
    let clean_journal_path = validate_absolute_no_symlink_components(journal_path)?;
    if clean_journal_path.parent() != Some(clean_dir.as_path()) {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal file must reside directly inside the private plan store directory.",
        ));
    }
    validate_preexisting_private_file(&clean_journal_path)?;

    let file = match File::open(&clean_journal_path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(_) => {
            return Err(WorkflowError::Io(
                "Failed to open workflow command journal.",
            ));
        }
    };

    let meta = fs::symlink_metadata(&clean_journal_path)
        .map_err(|_| WorkflowError::Io("Failed to read workflow command journal metadata."))?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal must be a regular non-symlink file.",
        ));
    }
    if meta.len() > MAX_COMMAND_JOURNAL_BYTES as u64 {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal file size exceeds the 1 MiB security limit.",
        ));
    }

    let file_meta = file
        .metadata()
        .map_err(|_| WorkflowError::Io("Failed to read open workflow command journal metadata."))?;
    if !file_meta.file_type().is_file() || file_meta.len() > MAX_COMMAND_JOURNAL_BYTES as u64 {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal open descriptor failed security validation.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if meta.dev() != file_meta.dev() || meta.ino() != file_meta.ino() {
            return Err(WorkflowError::SecurityViolation(
                "Workflow command journal file changed during open.",
            ));
        }
        if (file_meta.permissions().mode() & 0o077) != 0 {
            return Err(WorkflowError::SecurityViolation(
                "Workflow command journal file has insecure permissions (expected 0600).",
            ));
        }
    }

    let mut buf = Vec::new();
    file.take(MAX_COMMAND_JOURNAL_BYTES as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(|_| WorkflowError::Io("Failed to read workflow command journal."))?;
    if buf.len() > MAX_COMMAND_JOURNAL_BYTES {
        return Err(WorkflowError::SecurityViolation(
            "Workflow command journal exceeded the 1 MiB read bound.",
        ));
    }

    let Ok(journal) = serde_json::from_slice::<PersistedCommandJournal>(&buf) else {
        return Err(WorkflowError::CorruptStore(
            "Persisted workflow command journal is corrupt or incompatible.",
        ));
    };

    validate_journal(&journal, binding_id)?;
    Ok(Some(journal))
}

pub(crate) fn write_journal_locked(
    clean_dir: &Path,
    journal_path: &Path,
    journal: &PersistedCommandJournal,
) -> Result<(), WorkflowError> {
    validate_journal(journal, &journal.binding_id)?;
    validate_preexisting_private_file(journal_path)?;

    if journal.entries.len() > MAX_COMMAND_JOURNAL_ENTRIES {
        return Err(WorkflowError::InvalidInput(
            "Workflow command journal entry limit exceeded.",
        ));
    }

    let Ok(serialized) = serde_json::to_vec_pretty(journal) else {
        return Err(WorkflowError::Serialization(
            "Failed to serialize workflow command journal.",
        ));
    };
    if serialized.len() > MAX_COMMAND_JOURNAL_BYTES {
        return Err(WorkflowError::InvalidInput(
            "Workflow command journal byte size limit exceeded.",
        ));
    }

    let file_name = journal_path.file_name().and_then(|n| n.to_str()).ok_or(
        WorkflowError::SecurityViolation("Invalid workflow command journal file name."),
    )?;
    let temp_name = format!(".{file_name}.tmp.{}.{}", std::process::id(), Uuid::new_v4());
    let temp_path = clean_dir.join(temp_name);
    let _ = validate_absolute_no_symlink_components(&temp_path)?;

    let write_result = (|| -> Result<(), WorkflowError> {
        #[cfg(unix)]
        let mut temp_file = {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            let f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp_path)
                .map_err(|_| {
                    WorkflowError::Io("Failed to create temporary workflow command journal file.")
                })?;
            f.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| {
                    WorkflowError::Io("Failed to set permissions on temporary command journal.")
                })?;
            f
        };

        #[cfg(not(unix))]
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|_| {
                WorkflowError::Io("Failed to create temporary workflow command journal file.")
            })?;

        temp_file.write_all(&serialized).map_err(|_| {
            WorkflowError::Io("Failed to write temporary workflow command journal.")
        })?;
        temp_file.sync_all().map_err(|_| {
            WorkflowError::Io("Failed to fsync temporary workflow command journal.")
        })?;
        drop(temp_file);

        fs::rename(&temp_path, journal_path).map_err(|_| {
            WorkflowError::Io("Failed to atomically replace workflow command journal.")
        })?;
        let dir_file = File::open(clean_dir)
            .map_err(|_| WorkflowError::Io("Failed to open plan store directory for fsync."))?;
        dir_file.sync_all().map_err(|_| {
            WorkflowError::Io("Failed to fsync plan store directory after journal update.")
        })?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

fn validate_journal(
    journal: &PersistedCommandJournal,
    expected_binding_id: &str,
) -> Result<(), WorkflowError> {
    if journal.schema_version != WORKFLOW_JOURNAL_SCHEMA_VERSION {
        return Err(WorkflowError::CorruptStore(
            "Unsupported workflow command journal schema version.",
        ));
    }
    if journal.binding_id != expected_binding_id {
        return Err(WorkflowError::CorruptStore(
            "Workflow command journal binding identity mismatch.",
        ));
    }
    if journal.entries.len() > MAX_COMMAND_JOURNAL_ENTRIES {
        return Err(WorkflowError::CorruptStore(
            "Persisted workflow command journal exceeds maximum entry count.",
        ));
    }

    let mut seen_keys = BTreeSet::new();
    for entry in &journal.entries {
        if validate_identifier("command_key", &entry.command_key).is_err() {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains an invalid command_key.",
            ));
        }
        if !seen_keys.insert(entry.command_key.as_str()) {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains duplicate command_key entries.",
            ));
        }
        if validate_identifier("plan_id", &entry.resolved_plan_id).is_err() {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains an invalid resolved_plan_id.",
            ));
        }
        if let Some(explicit) = &entry.explicit_plan_id {
            if validate_identifier("plan_id", explicit).is_err()
                || explicit != &entry.resolved_plan_id
            {
                return Err(WorkflowError::CorruptStore(
                    "Persisted workflow command journal contains an inconsistent explicit_plan_id.",
                ));
            }
        }
        if validate_js_safe_revision("expected_revision", entry.expected_revision).is_err()
            || validate_js_safe_revision("target_revision", entry.target_revision).is_err()
        {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains an out-of-range revision number.",
            ));
        }
        if entry.target_revision == 0
            || entry.target_revision != entry.expected_revision.saturating_add(1)
        {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal has inconsistent expected and target revisions.",
            ));
        }
        if validate_identifier("profile_id", &entry.profile_id).is_err()
            || validate_js_safe_revision("profile_revision", entry.profile_revision).is_err()
            || validate_summary(&entry.summary).is_err()
        {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains invalid profile or summary metadata.",
            ));
        }
        if !is_hex_64(&entry.tasks_digest) || !is_hex_64(&entry.payload_digest) {
            return Err(WorkflowError::CorruptStore(
                "Persisted workflow command journal contains a malformed payload digest.",
            ));
        }
        if let Some(ref digest) = entry.plan_digest {
            if !is_hex_64(digest) {
                return Err(WorkflowError::CorruptStore(
                    "Persisted workflow command journal contains a malformed plan_digest.",
                ));
            }
        }
        if let Some(ref proof) = entry.scoped_profile_dependency_proof {
            if proof.validate().is_err()
                || proof.provenance_revision != entry.profile_revision
                || proof.spec.profile_id != entry.profile_id
            {
                return Err(WorkflowError::CorruptStore(
                    "Persisted workflow command journal contains an invalid scoped profile dependency proof.",
                ));
            }
        }
        if let CommandJournalState::Completed {
            created_revision,
            created_at_ms,
            ref plan_digest,
        } = entry.state
        {
            if created_revision != entry.target_revision || created_at_ms < 0 {
                return Err(WorkflowError::CorruptStore(
                    "Persisted workflow command journal contains an invalid completed record.",
                ));
            }
            if let Some(digest) = plan_digest {
                if !is_hex_64(digest) {
                    return Err(WorkflowError::CorruptStore(
                        "Persisted workflow command journal contains an invalid completed plan_digest.",
                    ));
                }
            }
            if let (Some(entry_digest), Some(completed_digest)) =
                (entry.plan_digest.as_deref(), plan_digest.as_deref())
            {
                if entry_digest != completed_digest {
                    return Err(WorkflowError::CorruptStore(
                        "Persisted workflow command journal contains inconsistent plan_digest values.",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn is_hex_64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn ensure_platform_supported() -> Result<(), WorkflowError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(WorkflowError::UnsupportedPlatform(
            "Current platform does not support required private store permissions.",
        ))
    }
}

fn resolve_clean_path(path: &Path) -> Result<PathBuf, WorkflowError> {
    if path.as_os_str().is_empty() {
        return Err(WorkflowError::SecurityViolation(
            "Store path must not be empty.",
        ));
    }
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| WorkflowError::Io("Failed to resolve current working directory."))?
            .join(path)
    };
    validate_absolute_no_symlink_components(&abs)
}

fn validate_absolute_no_symlink_components(path: &Path) -> Result<PathBuf, WorkflowError> {
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(WorkflowError::SecurityViolation(
            "Store path must be a non-empty absolute path.",
        ));
    }

    let mut current = PathBuf::new();
    let mut components = path.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(WorkflowError::SecurityViolation(
                    "Store path must not contain relative traversal components.",
                ));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(WorkflowError::SecurityViolation(
                                "Store path must not traverse symlink components.",
                            ));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(WorkflowError::SecurityViolation(
                                "Store parent component is not a directory.",
                            ));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(WorkflowError::Io(
                            "Failed to inspect store path component metadata.",
                        ));
                    }
                }
            }
        }
    }

    Ok(current)
}

fn ensure_private_dir(path: &Path) -> Result<PathBuf, WorkflowError> {
    let checked = resolve_clean_path(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let meta = fs::symlink_metadata(&checked)
            .map_err(|_| WorkflowError::Io("Failed to inspect plan store directory metadata."))?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(WorkflowError::SecurityViolation(
                "Plan store path must be a non-symlink directory.",
            ));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(WorkflowError::SecurityViolation(
                "Plan store directory has non-private permissions (expected 0700).",
            ));
        }
    }

    Ok(checked)
}

fn validate_preexisting_private_file(path: &Path) -> Result<(), WorkflowError> {
    let _ = validate_absolute_no_symlink_components(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(WorkflowError::SecurityViolation(
                        "Workflow command journal file must not be a symlink.",
                    ));
                }
                if !meta.file_type().is_file() {
                    return Err(WorkflowError::SecurityViolation(
                        "Workflow command journal path must be a regular file.",
                    ));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(WorkflowError::SecurityViolation(
                        "Workflow command journal file has insecure permissions (expected 0600).",
                    ));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(WorkflowError::Io(
                    "Failed to inspect workflow command journal file metadata.",
                ));
            }
        }
    }

    Ok(())
}

fn open_private_lock_file(path: &Path) -> Result<File, WorkflowError> {
    validate_preexisting_private_file(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(|_| WorkflowError::Io("Failed to open workflow command journal lock file."))?;

        let sym_meta = fs::symlink_metadata(path).map_err(|_| {
            WorkflowError::Io("Failed to inspect workflow command journal lock file.")
        })?;
        if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
            return Err(WorkflowError::SecurityViolation(
                "Workflow command journal lock file must be a regular non-symlink file.",
            ));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| {
                WorkflowError::Io(
                    "Failed to enforce private permissions on workflow command journal lock file.",
                )
            })?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        Err(WorkflowError::UnsupportedPlatform(
            "Current platform does not support required private store permissions.",
        ))
    }
}

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use fs2::FileExt;

use crate::error::RegistryError;
use crate::types::{
    Difficulty, ModelChoice, ModelDefinition, RegistryChange, RegistrySnapshot, RoleAssignment,
    ToolDefinition, WorkflowProfile, WorkflowRole, MAX_ASSIGNMENTS_PER_PROFILE,
    MAX_EFFORTS_PER_MODEL, MAX_EFFORT_LEN, MAX_EXECUTABLE_PATH_LEN, MAX_ID_LEN,
    MAX_JS_SAFE_INTEGER, MAX_LABEL_LEN, MAX_MODELS, MAX_MODEL_ARG_LEN, MAX_PROFILES,
    MAX_REGISTRY_FILE_BYTES, MAX_TOOLS, REGISTRY_FILE_NAME, REGISTRY_LOCK_FILE_NAME,
    REGISTRY_SCHEMA_VERSION,
};

/// Persistent, cross-process locked store for the global [`RegistrySnapshot`].
///
/// Compare-and-swap mutations and the standard [`RegistryStore::snapshot`] read are serialized
/// across threads and processes via a stable `fs2` lock file (`registry.lock`) inside the private
/// configuration directory (`0700` directory mode, `0600` file mode on Unix). Mutations reread the
/// latest on-disk snapshot under the exclusive lock, validate all reference and effort invariants,
/// and atomically replace `registry.json` via a same-directory temporary file with `fsync`.
/// [`RegistryStore::snapshot_readonly`] and
/// [`RegistryStore::read_snapshot_readonly_at`] reuse the same validated parser without opening
/// or creating the lock file.
#[derive(Debug, Clone)]
pub struct RegistryStore {
    config_dir: PathBuf,
    process_lock: Arc<Mutex<()>>,
}

impl RegistryStore {
    /// Opens or initializes a private registry configuration directory at `config_dir`.
    ///
    /// If `registry.json` does not exist yet, [`RegistryStore::snapshot`] returns an empty
    /// revision-0 [`RegistrySnapshot`] without writing any implicit default tools, models,
    /// or profiles.
    pub fn open(config_dir: impl AsRef<Path>) -> Result<Self, RegistryError> {
        ensure_platform_supported()?;
        let resolved_dir = prepare_config_dir(config_dir.as_ref())?;
        let lock_path = resolved_dir.join(REGISTRY_LOCK_FILE_NAME);
        let _lock_file = open_lock_file(&lock_path)?;

        let registry_path = resolved_dir.join(REGISTRY_FILE_NAME);
        validate_existing_registry_file_type(&registry_path)?;

        Ok(Self {
            config_dir: resolved_dir,
            process_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Returns the resolved registry configuration directory path.
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Returns the path to `registry.json` inside [`Self::config_dir`].
    pub fn registry_path(&self) -> PathBuf {
        self.config_dir.join(REGISTRY_FILE_NAME)
    }

    /// Returns the path to `registry.lock` inside [`Self::config_dir`].
    pub fn lock_path(&self) -> PathBuf {
        self.config_dir.join(REGISTRY_LOCK_FILE_NAME)
    }

    /// Reads and validates the current [`RegistrySnapshot`] under a shared cross-process lock.
    ///
    /// Returns an empty revision-0 snapshot if `registry.json` does not exist yet.
    pub fn snapshot(&self) -> Result<RegistrySnapshot, RegistryError> {
        let _guard = self.acquire_lock(false)?;
        self.read_snapshot_locked()
    }

    /// Reads and validates the current [`RegistrySnapshot`] without acquiring or creating
    /// `registry.lock`.
    ///
    /// Uses the same parser and existing private-directory validation as [`Self::snapshot`].
    /// This read-only route never calls `prepare_config_dir` or `open_lock_file`, never creates
    /// a missing configuration directory, lock file, or registry file, and requires the
    /// configuration directory to already exist. Atomic `registry.json` replacement yields
    /// either the previous or the next complete snapshot.
    pub fn snapshot_readonly(&self) -> Result<RegistrySnapshot, RegistryError> {
        self.read_snapshot_locked()
    }

    /// Reads and validates the [`RegistrySnapshot`] in `config_dir` without acquiring or creating
    /// `registry.lock`.
    ///
    /// The configuration directory must already exist. This static read-only route never calls
    /// `prepare_config_dir` or `open_lock_file`, never creates a missing directory, lock file, or
    /// registry file, and uses the same JSON/schema/integrity parser and private-directory
    /// validation as [`Self::snapshot`].
    pub fn read_snapshot_readonly_at(
        config_dir: impl AsRef<Path>,
    ) -> Result<RegistrySnapshot, RegistryError> {
        ensure_platform_supported()?;
        let resolved_dir = validate_no_symlink_components(config_dir.as_ref())?;
        let store = Self {
            config_dir: resolved_dir,
            process_lock: Arc::new(Mutex::new(())),
        };
        store.read_snapshot_locked()
    }

    /// Applies `change` atomically if the on-disk revision equals `expected_revision`.
    ///
    /// Rereads `registry.json` under an exclusive cross-process lock, verifies
    /// `expected_revision`, validates all entity bounds, references, and effort constraints,
    /// increments `revision` by `1`, and atomically replaces `registry.json` in the same
    /// directory with `fsync`.
    pub fn apply(
        &self,
        expected_revision: u64,
        change: RegistryChange,
    ) -> Result<RegistrySnapshot, RegistryError> {
        if expected_revision > MAX_JS_SAFE_INTEGER {
            return Err(RegistryError::InvalidInput(
                "Expected revision exceeds maximum JS-safe integer.",
            ));
        }

        let _guard = self.acquire_lock(true)?;
        let current = self.read_snapshot_locked()?;

        if current.revision != expected_revision {
            return Err(RegistryError::StaleRevision {
                expected: expected_revision,
                actual: current.revision,
            });
        }

        if current.revision >= MAX_JS_SAFE_INTEGER {
            return Err(RegistryError::InvalidInput(
                "Registry revision cannot be incremented beyond maximum JS-safe integer.",
            ));
        }
        let next_revision = current.revision + 1;

        let mut candidate = current;
        apply_change_in_memory(&mut candidate, change)?;
        candidate.schema_version = REGISTRY_SCHEMA_VERSION;
        candidate.revision = next_revision;
        validate_snapshot(&candidate)?;

        self.write_snapshot_atomic_locked(&candidate)?;
        Ok(candidate)
    }

    fn acquire_lock(&self, exclusive: bool) -> Result<StoreLockGuard<'_>, RegistryError> {
        let mutex_guard = self
            .process_lock
            .lock()
            .map_err(|_| RegistryError::Io("Registry in-process mutex is poisoned."))?;

        verify_existing_config_dir(&self.config_dir)?;
        let lock_path = self.lock_path();
        let lock_file = open_lock_file(&lock_path)?;

        if exclusive {
            lock_file.lock_exclusive().map_err(|_| {
                RegistryError::Io("Failed to acquire exclusive registry file lock.")
            })?;
        } else {
            lock_file
                .lock_shared()
                .map_err(|_| RegistryError::Io("Failed to acquire shared registry file lock."))?;
        }

        Ok(StoreLockGuard {
            _mutex_guard: mutex_guard,
            lock_file,
        })
    }

    fn read_snapshot_locked(&self) -> Result<RegistrySnapshot, RegistryError> {
        verify_existing_config_dir(&self.config_dir)?;
        let registry_path = self.registry_path();

        let sym_meta = match fs::symlink_metadata(&registry_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(RegistrySnapshot::empty());
            }
            Err(_) => {
                return Err(RegistryError::Io(
                    "Failed to inspect registry snapshot metadata.",
                ));
            }
        };

        if sym_meta.file_type().is_symlink() {
            return Err(RegistryError::SecurityViolation(
                "Registry snapshot file must not be a symlink.",
            ));
        }
        if !sym_meta.file_type().is_file() {
            return Err(RegistryError::SecurityViolation(
                "Registry snapshot path must be a regular file.",
            ));
        }
        if sym_meta.len() > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::CorruptStore(
                "Registry snapshot file exceeds the 1 MiB size limit.",
            ));
        }

        let file = File::open(&registry_path)
            .map_err(|_| RegistryError::Io("Failed to open registry snapshot file."))?;
        let open_meta = file
            .metadata()
            .map_err(|_| RegistryError::Io("Failed to read opened registry snapshot metadata."))?;

        if !open_meta.file_type().is_file() || !same_file_identity(&sym_meta, &open_meta) {
            return Err(RegistryError::SecurityViolation(
                "Registry snapshot file changed unexpectedly during open.",
            ));
        }

        let capacity = (open_meta.len().min(MAX_REGISTRY_FILE_BYTES) as usize).saturating_add(1);
        let mut buf = Vec::with_capacity(capacity);
        file.take(MAX_REGISTRY_FILE_BYTES + 1)
            .read_to_end(&mut buf)
            .map_err(|_| RegistryError::Io("Failed to read registry snapshot file."))?;

        if (buf.len() as u64) > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::CorruptStore(
                "Registry snapshot file exceeds the 1 MiB size limit.",
            ));
        }

        let snapshot: RegistrySnapshot = serde_json::from_slice(&buf).map_err(|_| {
            RegistryError::CorruptStore("Registry snapshot file contains malformed JSON.")
        })?;

        if snapshot.schema_version != REGISTRY_SCHEMA_VERSION {
            return Err(RegistryError::CorruptStore(
                "Registry snapshot has an unsupported schema version.",
            ));
        }
        if snapshot.revision > MAX_JS_SAFE_INTEGER {
            return Err(RegistryError::CorruptStore(
                "Registry snapshot revision exceeds maximum JS-safe integer.",
            ));
        }

        validate_snapshot(&snapshot).map_err(|_| {
            RegistryError::CorruptStore("Stored registry snapshot failed integrity validation.")
        })?;

        Ok(snapshot)
    }

    fn write_snapshot_atomic_locked(
        &self,
        snapshot: &RegistrySnapshot,
    ) -> Result<(), RegistryError> {
        verify_existing_config_dir(&self.config_dir)?;
        let registry_path = self.registry_path();
        validate_existing_registry_file_type(&registry_path)?;

        let mut encoded = serde_json::to_vec_pretty(snapshot)
            .map_err(|_| RegistryError::Io("Failed to serialize registry snapshot."))?;
        encoded.push(b'\n');

        if (encoded.len() as u64) > MAX_REGISTRY_FILE_BYTES {
            return Err(RegistryError::CapacityExceeded(
                "Serialized registry snapshot exceeds the 1 MiB size limit.",
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut temp = tempfile::Builder::new()
                .prefix(".registry.json.tmp.")
                .permissions(fs::Permissions::from_mode(0o600))
                .tempfile_in(&self.config_dir)
                .map_err(|_| {
                    RegistryError::Io("Failed to create temporary registry file in config dir.")
                })?;

            let temp_sym_meta = fs::symlink_metadata(temp.path()).map_err(|_| {
                RegistryError::Io("Failed to inspect temporary registry file metadata.")
            })?;
            if temp_sym_meta.file_type().is_symlink() || !temp_sym_meta.file_type().is_file() {
                return Err(RegistryError::SecurityViolation(
                    "Temporary registry file must be a regular non-symlink file.",
                ));
            }

            temp.write_all(&encoded)
                .map_err(|_| RegistryError::Io("Failed to write temporary registry file."))?;
            temp.flush()
                .map_err(|_| RegistryError::Io("Failed to flush temporary registry file."))?;
            temp.as_file()
                .sync_all()
                .map_err(|_| RegistryError::Io("Failed to fsync temporary registry file."))?;
            temp.as_file()
                .set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| {
                    RegistryError::Io("Failed to set permissions on temporary registry file.")
                })?;

            validate_existing_registry_file_type(&registry_path)?;

            temp.persist(&registry_path).map_err(|_| {
                RegistryError::Io("Failed to atomically replace registry snapshot file.")
            })?;
            fs::set_permissions(&registry_path, fs::Permissions::from_mode(0o600)).map_err(
                |_| RegistryError::Io("Failed to enforce 0600 permissions on registry snapshot."),
            )?;

            let dir_file = File::open(&self.config_dir)
                .map_err(|_| RegistryError::Io("Failed to open registry directory for fsync."))?;
            dir_file
                .sync_all()
                .map_err(|_| RegistryError::Io("Failed to fsync registry directory."))?;
        }

        #[cfg(not(unix))]
        {
            let _ = (&encoded, &registry_path);
            ensure_platform_supported()?;
        }

        Ok(())
    }
}

struct StoreLockGuard<'a> {
    _mutex_guard: MutexGuard<'a, ()>,
    lock_file: File,
}

impl Drop for StoreLockGuard<'_> {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

/// Resolves the [`ModelChoice`] for a given `profile_id`, `role`, and optional `difficulty`.
///
/// Resolution checks for an exact `(role, Some(difficulty))` match first (when `difficulty` is
/// `Some`), then falls back to the explicit role default `(role, None)`. It never substitutes
/// another difficulty, another role, or an unapproved model. Returns an explicit error if the
/// profile is missing, the assignment is missing, or the referenced model/effort is invalid.
pub fn resolve_choice(
    snapshot: &RegistrySnapshot,
    profile_id: &str,
    role: WorkflowRole,
    difficulty: Option<Difficulty>,
) -> Result<ModelChoice, RegistryError> {
    validate_identifier(profile_id)?;

    let profile = snapshot
        .profiles
        .iter()
        .find(|candidate| candidate.id == profile_id)
        .ok_or(RegistryError::NotFound(
            "Workflow profile not found in registry snapshot.",
        ))?;

    validate_unique_assignments(&profile.assignments)?;

    let assignment = match difficulty {
        Some(target_difficulty) => profile
            .assignments
            .iter()
            .find(|entry| entry.role == role && entry.difficulty == Some(target_difficulty))
            .or_else(|| {
                profile
                    .assignments
                    .iter()
                    .find(|entry| entry.role == role && entry.difficulty.is_none())
            }),
        None => profile
            .assignments
            .iter()
            .find(|entry| entry.role == role && entry.difficulty.is_none()),
    }
    .ok_or(RegistryError::MissingAssignment(
        "No model assignment is configured for the requested role and difficulty.",
    ))?;

    validate_model_choice(&assignment.choice, &snapshot.models, &snapshot.tools)?;
    Ok(assignment.choice.clone())
}

fn apply_change_in_memory(
    snapshot: &mut RegistrySnapshot,
    change: RegistryChange,
) -> Result<(), RegistryError> {
    match change {
        RegistryChange::UpsertTool { tool } => {
            validate_tool_definition(&tool)?;
            if let Some(existing) = snapshot.tools.iter_mut().find(|entry| entry.id == tool.id) {
                *existing = tool;
            } else {
                if snapshot.tools.len() >= MAX_TOOLS {
                    return Err(RegistryError::CapacityExceeded(
                        "Registry tool limit (64) exceeded.",
                    ));
                }
                snapshot.tools.push(tool);
            }
        }
        RegistryChange::DeleteTool { tool_id } => {
            validate_identifier(&tool_id)?;
            if !snapshot.tools.iter().any(|entry| entry.id == tool_id) {
                return Err(RegistryError::NotFound("Tool not found in registry."));
            }
            if snapshot.models.iter().any(|model| model.tool_id == tool_id) {
                return Err(RegistryError::ReferenceConflict(
                    "Cannot delete tool while models still reference it.",
                ));
            }
            snapshot.tools.retain(|entry| entry.id != tool_id);
        }
        RegistryChange::UpsertModel { model } => {
            upsert_single_model(snapshot, model)?;
        }
        RegistryChange::UpsertModels { models } => {
            if models.len() > MAX_MODELS {
                return Err(RegistryError::CapacityExceeded(
                    "Registry model limit (256) exceeded.",
                ));
            }
            let mut batch_ids = BTreeSet::new();
            for model in &models {
                validate_model_definition(model)?;
                if !batch_ids.insert(model.id.as_str()) {
                    return Err(RegistryError::InvalidInput(
                        "Batch model upsert contains duplicate model identifiers.",
                    ));
                }
            }
            for model in models {
                upsert_single_model(snapshot, model)?;
            }
        }
        RegistryChange::DeleteModel { model_id } => {
            validate_identifier(&model_id)?;
            if !snapshot.models.iter().any(|entry| entry.id == model_id) {
                return Err(RegistryError::NotFound("Model not found in registry."));
            }
            let referenced_by_profile = snapshot.profiles.iter().any(|profile| {
                profile
                    .assignments
                    .iter()
                    .any(|assignment| assignment.choice.model_id == model_id)
            });
            if referenced_by_profile {
                return Err(RegistryError::ReferenceConflict(
                    "Cannot delete model while workflow profiles still reference it.",
                ));
            }
            snapshot.models.retain(|entry| entry.id != model_id);
        }
        RegistryChange::UpsertProfile { profile } => {
            validate_workflow_profile(&profile, &snapshot.models, &snapshot.tools)?;
            if let Some(existing) = snapshot
                .profiles
                .iter_mut()
                .find(|entry| entry.id == profile.id)
            {
                *existing = profile;
            } else {
                if snapshot.profiles.len() >= MAX_PROFILES {
                    return Err(RegistryError::CapacityExceeded(
                        "Registry workflow profile limit (64) exceeded.",
                    ));
                }
                snapshot.profiles.push(profile);
            }
        }
        RegistryChange::DeleteProfile { profile_id } => {
            validate_identifier(&profile_id)?;
            if !snapshot.profiles.iter().any(|entry| entry.id == profile_id) {
                return Err(RegistryError::NotFound(
                    "Workflow profile not found in registry.",
                ));
            }
            snapshot.profiles.retain(|entry| entry.id != profile_id);
            if snapshot.default_profile_id.as_deref() == Some(profile_id.as_str()) {
                snapshot.default_profile_id = None;
            }
        }
        RegistryChange::SetDefaultProfile { profile_id } => match profile_id {
            Some(id) => {
                validate_identifier(&id)?;
                if !snapshot.profiles.iter().any(|entry| entry.id == id) {
                    return Err(RegistryError::InvalidReference(
                        "Default profile references a workflow profile that does not exist.",
                    ));
                }
                snapshot.default_profile_id = Some(id);
            }
            None => {
                snapshot.default_profile_id = None;
            }
        },
    }

    Ok(())
}

fn upsert_single_model(
    snapshot: &mut RegistrySnapshot,
    model: ModelDefinition,
) -> Result<(), RegistryError> {
    validate_model_definition(&model)?;
    if !snapshot.tools.iter().any(|tool| tool.id == model.tool_id) {
        return Err(RegistryError::InvalidReference(
            "Model references a tool that does not exist in the registry.",
        ));
    }

    if let Some(existing) = snapshot
        .models
        .iter_mut()
        .find(|entry| entry.id == model.id)
    {
        *existing = model;
    } else {
        if snapshot.models.len() >= MAX_MODELS {
            return Err(RegistryError::CapacityExceeded(
                "Registry model limit (256) exceeded.",
            ));
        }
        snapshot.models.push(model);
    }

    Ok(())
}

fn validate_snapshot(snapshot: &RegistrySnapshot) -> Result<(), RegistryError> {
    if snapshot.schema_version != REGISTRY_SCHEMA_VERSION {
        return Err(RegistryError::InvalidInput(
            "Unsupported registry schema version.",
        ));
    }
    if snapshot.revision > MAX_JS_SAFE_INTEGER {
        return Err(RegistryError::InvalidInput(
            "Registry revision exceeds maximum JS-safe integer.",
        ));
    }
    if snapshot.tools.len() > MAX_TOOLS {
        return Err(RegistryError::CapacityExceeded(
            "Registry tool limit (64) exceeded.",
        ));
    }
    if snapshot.models.len() > MAX_MODELS {
        return Err(RegistryError::CapacityExceeded(
            "Registry model limit (256) exceeded.",
        ));
    }
    if snapshot.profiles.len() > MAX_PROFILES {
        return Err(RegistryError::CapacityExceeded(
            "Registry workflow profile limit (64) exceeded.",
        ));
    }

    let mut tool_ids = BTreeSet::new();
    for tool in &snapshot.tools {
        validate_tool_definition(tool)?;
        if !tool_ids.insert(tool.id.as_str()) {
            return Err(RegistryError::InvalidInput(
                "Registry contains duplicate tool identifiers.",
            ));
        }
    }

    let mut model_ids = BTreeSet::new();
    for model in &snapshot.models {
        validate_model_definition(model)?;
        if !model_ids.insert(model.id.as_str()) {
            return Err(RegistryError::InvalidInput(
                "Registry contains duplicate model identifiers.",
            ));
        }
        if !tool_ids.contains(model.tool_id.as_str()) {
            return Err(RegistryError::InvalidReference(
                "Model references a tool that does not exist in the registry.",
            ));
        }
    }

    let mut profile_ids = BTreeSet::new();
    for profile in &snapshot.profiles {
        validate_workflow_profile(profile, &snapshot.models, &snapshot.tools)?;
        if !profile_ids.insert(profile.id.as_str()) {
            return Err(RegistryError::InvalidInput(
                "Registry contains duplicate workflow profile identifiers.",
            ));
        }
    }

    if let Some(ref default_id) = snapshot.default_profile_id {
        validate_identifier(default_id)?;
        if !profile_ids.contains(default_id.as_str()) {
            return Err(RegistryError::InvalidReference(
                "Default profile references a workflow profile that does not exist.",
            ));
        }
    }

    Ok(())
}

fn validate_tool_definition(tool: &ToolDefinition) -> Result<(), RegistryError> {
    validate_identifier(&tool.id)?;
    validate_bounded_text(&tool.label, MAX_LABEL_LEN)?;

    if let Some(ref exec_path) = tool.executable_path {
        let raw = exec_path.as_os_str();
        if raw.is_empty() {
            return Err(RegistryError::InvalidInput(
                "Tool executable path must not be empty when provided.",
            ));
        }
        let path_str = exec_path.to_str().ok_or(RegistryError::InvalidInput(
            "Tool executable path must be valid UTF-8.",
        ))?;
        if path_str.trim() != path_str
            || path_str.len() > MAX_EXECUTABLE_PATH_LEN
            || path_str.chars().any(char::is_control)
        {
            return Err(RegistryError::InvalidInput(
                "Tool executable path exceeds bounds or contains control characters.",
            ));
        }
    }

    Ok(())
}

fn validate_model_definition(model: &ModelDefinition) -> Result<(), RegistryError> {
    validate_identifier(&model.id)?;
    validate_identifier(&model.tool_id)?;
    validate_bounded_text(&model.label, MAX_LABEL_LEN)?;
    validate_bounded_text(&model.model_arg, MAX_MODEL_ARG_LEN)?;

    if model.efforts.len() > MAX_EFFORTS_PER_MODEL {
        return Err(RegistryError::CapacityExceeded(
            "Model effort list exceeds maximum length.",
        ));
    }

    let mut seen_efforts = BTreeSet::new();
    for effort in &model.efforts {
        validate_bounded_text(effort, MAX_EFFORT_LEN)?;
        if !seen_efforts.insert(effort.as_str()) {
            return Err(RegistryError::InvalidInput(
                "Model definition contains duplicate effort levels.",
            ));
        }
    }

    Ok(())
}

fn validate_workflow_profile(
    profile: &WorkflowProfile,
    models: &[ModelDefinition],
    tools: &[ToolDefinition],
) -> Result<(), RegistryError> {
    validate_identifier(&profile.id)?;
    validate_bounded_text(&profile.name, MAX_LABEL_LEN)?;

    if profile.assignments.len() > MAX_ASSIGNMENTS_PER_PROFILE {
        return Err(RegistryError::CapacityExceeded(
            "Workflow profile assignment count exceeds maximum (30).",
        ));
    }

    validate_unique_assignments(&profile.assignments)?;

    for assignment in &profile.assignments {
        validate_model_choice(&assignment.choice, models, tools)?;
    }

    Ok(())
}

fn validate_unique_assignments(assignments: &[RoleAssignment]) -> Result<(), RegistryError> {
    let mut seen = BTreeSet::new();
    for assignment in assignments {
        if !seen.insert((assignment.role, assignment.difficulty)) {
            return Err(RegistryError::DuplicateAssignment(
                "Workflow profile contains duplicate role and difficulty assignments.",
            ));
        }
    }
    Ok(())
}

fn validate_model_choice(
    choice: &ModelChoice,
    models: &[ModelDefinition],
    tools: &[ToolDefinition],
) -> Result<(), RegistryError> {
    validate_identifier(&choice.model_id)?;
    if let Some(ref effort) = choice.effort {
        validate_bounded_text(effort, MAX_EFFORT_LEN)?;
    }

    let model = models
        .iter()
        .find(|candidate| candidate.id == choice.model_id)
        .ok_or(RegistryError::InvalidReference(
            "Assigned model does not exist in the registry.",
        ))?;

    if !tools.iter().any(|tool| tool.id == model.tool_id) {
        return Err(RegistryError::InvalidReference(
            "Assigned model references a tool that does not exist in the registry.",
        ));
    }

    match choice.effort {
        Some(ref effort) => {
            if !model.efforts.iter().any(|supported| supported == effort) {
                return Err(RegistryError::UnsupportedEffort(
                    "Assigned effort is not supported by the selected model.",
                ));
            }
        }
        None => {
            if !model.allow_no_effort {
                return Err(RegistryError::UnsupportedEffort(
                    "Selected model requires an explicit effort level.",
                ));
            }
        }
    }

    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), RegistryError> {
    validate_bounded_text(value, MAX_ID_LEN)
}

fn validate_bounded_text(value: &str, max_chars: usize) -> Result<(), RegistryError> {
    if value.is_empty() || value.trim() != value {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field must be non-empty and trimmed.",
        ));
    }
    let char_count = value.chars().count();
    if char_count == 0 || char_count > max_chars {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field exceeds maximum permitted length.",
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(RegistryError::InvalidInput(
            "Identifier or text field must not contain control characters.",
        ));
    }
    Ok(())
}

fn ensure_platform_supported() -> Result<(), RegistryError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(RegistryError::SecurityViolation(
            "Private registry directory permissions (0700/0600) require a Unix platform.",
        ))
    }
}

fn validate_no_symlink_components(path: &Path) -> Result<PathBuf, RegistryError> {
    if path.as_os_str().is_empty() {
        return Err(RegistryError::InvalidInput(
            "Registry configuration directory path must not be empty.",
        ));
    }

    for comp in path.components() {
        if matches!(comp, Component::ParentDir) {
            return Err(RegistryError::SecurityViolation(
                "Registry configuration path must not contain parent directory traversal (`..`).",
            ));
        }
    }

    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let cwd = std::env::current_dir()
            .map_err(|_| RegistryError::Io("Failed to resolve current working directory."))?;
        cwd.join(path)
    };

    let mut current = PathBuf::new();
    let mut components = resolved.components().peekable();
    while let Some(comp) = components.next() {
        match comp {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(comp.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir => {
                return Err(RegistryError::SecurityViolation(
                    "Registry configuration path must not contain `..` components.",
                ));
            }
            Component::Normal(part) => {
                current.push(part);
                let is_last = components.peek().is_none();
                match fs::symlink_metadata(&current) {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            return Err(RegistryError::SecurityViolation(
                                "Registry configuration path must not traverse a symlink.",
                            ));
                        }
                        if !is_last && !meta.file_type().is_dir() {
                            return Err(RegistryError::SecurityViolation(
                                "Registry configuration parent component is not a directory.",
                            ));
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(RegistryError::Io(
                            "Failed to inspect registry configuration path metadata.",
                        ));
                    }
                }
            }
        }
    }

    Ok(resolved)
}

fn prepare_config_dir(config_dir: &Path) -> Result<PathBuf, RegistryError> {
    let resolved = validate_no_symlink_components(config_dir)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut current = PathBuf::new();
        for comp in resolved.components() {
            match comp {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir => continue,
                Component::ParentDir => {
                    return Err(RegistryError::SecurityViolation(
                        "Registry configuration path must not contain `..` components.",
                    ));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(RegistryError::SecurityViolation(
                                    "Registry configuration path must not traverse a symlink.",
                                ));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(RegistryError::SecurityViolation(
                                    "Registry configuration path component is not a directory.",
                                ));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current).map_err(|_| {
                                RegistryError::Io("Failed to create registry configuration dir.")
                            })?;
                        }
                        Err(_) => {
                            return Err(RegistryError::Io(
                                "Failed to inspect registry configuration directory.",
                            ));
                        }
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&resolved)
            .map_err(|_| RegistryError::Io("Failed to inspect registry configuration dir."))?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(RegistryError::SecurityViolation(
                "Registry configuration directory must be a non-symlink directory.",
            ));
        }

        fs::set_permissions(&resolved, fs::Permissions::from_mode(0o700)).map_err(|_| {
            RegistryError::Io("Failed to enforce 0700 permissions on registry directory.")
        })?;
    }

    Ok(resolved)
}

fn verify_existing_config_dir(config_dir: &Path) -> Result<(), RegistryError> {
    let _ = validate_no_symlink_components(config_dir)?;
    let meta = fs::symlink_metadata(config_dir)
        .map_err(|_| RegistryError::Io("Registry configuration directory is inaccessible."))?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(RegistryError::SecurityViolation(
            "Registry configuration directory must be a non-symlink directory.",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            fs::set_permissions(config_dir, fs::Permissions::from_mode(0o700)).map_err(|_| {
                RegistryError::SecurityViolation(
                    "Registry configuration directory has insecure permissions.",
                )
            })?;
        }
    }

    Ok(())
}

fn validate_existing_registry_file_type(path: &Path) -> Result<(), RegistryError> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(RegistryError::SecurityViolation(
                    "Registry file must not be a symlink.",
                ));
            }
            if !meta.file_type().is_file() {
                return Err(RegistryError::SecurityViolation(
                    "Registry path must be a regular file.",
                ));
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(RegistryError::Io("Failed to inspect registry file.")),
    }
}

fn open_lock_file(lock_path: &Path) -> Result<File, RegistryError> {
    validate_existing_registry_file_type(lock_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(lock_path)
            .map_err(|_| RegistryError::Io("Failed to open registry lock file."))?;

        let open_meta = file
            .metadata()
            .map_err(|_| RegistryError::Io("Failed to inspect opened registry lock file."))?;
        let sym_meta = fs::symlink_metadata(lock_path)
            .map_err(|_| RegistryError::Io("Failed to inspect registry lock file metadata."))?;

        if sym_meta.file_type().is_symlink()
            || !open_meta.file_type().is_file()
            || !same_file_identity(&sym_meta, &open_meta)
        {
            return Err(RegistryError::SecurityViolation(
                "Registry lock path must be a regular non-symlink file.",
            ));
        }

        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| RegistryError::Io("Failed to set 0600 permissions on registry lock."))?;

        Ok(file)
    }

    #[cfg(not(unix))]
    {
        let _ = lock_path;
        ensure_platform_supported()?;
        unreachable!()
    }
}

fn same_file_identity(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}

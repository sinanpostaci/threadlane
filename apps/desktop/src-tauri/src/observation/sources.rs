use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(unix)]
use std::fs::{self, DirBuilder, File, OpenOptions};
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
#[cfg(unix)]
use std::path::Component;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
#[cfg(unix)]
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use sha2::{Digest, Sha256};
#[cfg(unix)]
use threadlane_authority::validate_canonical_thread_uuid;
#[cfg(unix)]
use threadlane_core::observation::{
    validate_existing_private_dir_readonly, validate_existing_private_file_readonly,
};
use threadlane_core::observation::{ObservationSource, ObservationSourceAvailability};
#[cfg(unix)]
use threadlane_native::native_chat_observation::compute_observation_source_id;

#[cfg(unix)]
const MAX_CONTEXT_FILE_BYTES: u64 = 1024 * 1024;
#[cfg(unix)]
const MAX_LINKED_CONTEXT_REGISTRY_BYTES: u64 = 64 * 1024;
#[cfg(unix)]
const MAX_LINKED_CONTEXTS: usize = 50;
#[cfg(unix)]
const MAX_BINDING_ID_BYTES: usize = 128;

#[cfg(unix)]
const OBSERVATIONS_DIR_NAME: &str = "observations";
#[cfg(unix)]
const LINKED_CONTEXTS_FILE_NAME: &str = "linked-contexts.json";
#[cfg(unix)]
const LINKED_CONTEXTS_LOCK_FILE_NAME: &str = "linked-contexts.lock";

const LOCAL_SOURCE_LABEL: &str = "Threadlane local";
#[cfg(unix)]
const LINKED_SOURCE_LABEL: &str = "Linked Threadlane source";

#[cfg(not(unix))]
const UNSUPPORTED_PLATFORM_SOURCE_ID: &str =
    "src_0000000000000000000000000000000000000000000000000000000000000000";
#[cfg(not(unix))]
const UNSUPPORTED_PLATFORM_MESSAGE: &str =
    "Observation source registry is unsupported on this platform.";

#[cfg(unix)]
const EXPECTED_CONTEXT_ROUTE: &str = "trusted_local_operator";
#[cfg(unix)]
const NIL_UUID: &str = "00000000-0000-0000-0000-000000000000";

#[cfg(unix)]
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservationSourceConfig {
    pub(crate) runtime_state_dir: Option<PathBuf>,
    pub(crate) registry_dir: PathBuf,
    pub(crate) plans_dir: PathBuf,
    pub(crate) authority_dir: PathBuf,
    pub(crate) sessions_dir: PathBuf,
    pub(crate) app_data_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceSpec {
    pub(crate) source: ObservationSource,
    pub(crate) registry_dir: PathBuf,
    pub(crate) plans_dir: PathBuf,
    pub(crate) authority_dir: PathBuf,
    pub(crate) sessions_dir: PathBuf,
    pub(crate) runtime_state_dirs: Vec<PathBuf>,
    pub(crate) project_root_hint: Option<PathBuf>,
    pub(crate) binding_id_hint: Option<String>,
    pub(crate) thread_uuid_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SourceInventory {
    pub(crate) sources: Vec<ObservationSource>,
    pub(crate) specs: Vec<SourceSpec>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObservationSourceError {
    InvalidContextPath,
    InaccessibleContextFile,
    InvalidContextFormat,
    InvalidSchemaVersion,
    ForbiddenContextAlias,
    InvalidContextRoute,
    InvalidContextIdentityFlag,
    InvalidBindingId,
    InvalidThreadUuid,
    InvalidProjectRoot,
    InvalidStoreDirectory,
    OverlappingStoreDirectories,
    ProjectRootOverlapsStore,
    InvalidAppDataDirectory,
    InvalidLinkRegistry,
    LinkRegistryLimitReached,
    LinkRegistryWriteFailed,
    #[cfg(not(unix))]
    UnsupportedPlatform,
}

impl ObservationSourceError {
    pub(crate) fn message(&self) -> &'static str {
        match self {
            Self::InvalidContextPath => {
                "Context path must be an absolute canonical private file path without symlinks."
            }
            Self::InaccessibleContextFile => {
                "Context file is missing, inaccessible, or does not satisfy private file requirements."
            }
            Self::InvalidContextFormat => {
                "Context file is not a valid Threadlane private context document."
            }
            Self::InvalidSchemaVersion => "Context schema version is unsupported.",
            Self::ForbiddenContextAlias => {
                "Context file contains a forbidden top-level store or target alias."
            }
            Self::InvalidContextRoute => "Context route must be trusted_local_operator.",
            Self::InvalidContextIdentityFlag => {
                "Context native_per_call_identity_verified flag must be false."
            }
            Self::InvalidBindingId => "Context binding_id is invalid.",
            Self::InvalidThreadUuid => "Context thread_uuid must be a canonical non-nil UUID.",
            Self::InvalidProjectRoot => {
                "Context project_root must be an existing canonical directory without symlinks."
            }
            Self::InvalidStoreDirectory => {
                "Context store directories must be existing canonical private 0700 directories."
            }
            Self::OverlappingStoreDirectories => {
                "Context store directories must be pairwise distinct and non-overlapping."
            }
            Self::ProjectRootOverlapsStore => {
                "Context project_root must be disjoint from all Threadlane store directories."
            }
            Self::InvalidAppDataDirectory => {
                "Application data directory is missing or fails private directory validation."
            }
            Self::InvalidLinkRegistry => {
                "Linked context registry is malformed or fails private file validation."
            }
            Self::LinkRegistryLimitReached => {
                "Linked context registry has reached the maximum number of linked sources."
            }
            Self::LinkRegistryWriteFailed => {
                "Failed to persist the linked context registry atomically."
            }
            #[cfg(not(unix))]
            Self::UnsupportedPlatform => {
                "Observation source linking is unsupported on this platform."
            }
        }
    }
}

impl fmt::Display for ObservationSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ObservationSourceError {}

#[derive(Debug, Clone)]
pub(crate) struct ObservationSourceRegistry {
    config: Arc<Mutex<ObservationSourceConfig>>,
}

impl ObservationSourceRegistry {
    pub(crate) fn new(config: ObservationSourceConfig) -> Self {
        Self {
            config: Arc::new(Mutex::new(config)),
        }
    }

    pub(crate) fn config(&self) -> ObservationSourceConfig {
        self.config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn set_runtime_state_dir(&self, runtime_state_dir: Option<PathBuf>) {
        let mut guard = self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.runtime_state_dir = runtime_state_dir;
    }

    #[cfg(unix)]
    pub(crate) fn inventory(&self) -> SourceInventory {
        let config = self.config();
        let mut warnings = Vec::new();
        let mut builders: BTreeMap<String, SourceSpecAccumulator> = BTreeMap::new();
        let mut unavailable_sources: BTreeMap<String, ObservationSource> = BTreeMap::new();

        match validate_default_source(&config) {
            Ok(default_validated) => {
                if let Some(warning) = default_validated.runtime_warning {
                    push_unique_warning(&mut warnings, warning);
                }
                let entry = builders
                    .entry(default_validated.source_id.clone())
                    .or_insert_with(|| {
                        SourceSpecAccumulator::new(
                            default_validated.source_id,
                            true,
                            default_validated.registry_dir,
                            default_validated.plans_dir,
                            default_validated.authority_dir,
                            default_validated.sessions_dir,
                        )
                    });
                entry.is_local_default = true;
                if let Some(rt_dir) = default_validated.runtime_state_dir {
                    entry.push_runtime_state_dir(rt_dir);
                }
            }
            Err(default_unavailable) => {
                push_unique_warning(&mut warnings, default_unavailable.warning);
                unavailable_sources.insert(
                    default_unavailable.source_id.clone(),
                    ObservationSource {
                        id: default_unavailable.source_id,
                        label: LOCAL_SOURCE_LABEL.to_string(),
                        project_path: None,
                        availability: ObservationSourceAvailability::Unavailable,
                        message: Some(
                            "Local Threadlane durable stores are unavailable.".to_string(),
                        ),
                    },
                );
            }
        }

        match read_linked_context_entries_readonly(&config.app_data_dir) {
            Ok(entries) => {
                let mut seen_context_paths = BTreeSet::new();
                for entry in entries {
                    if !seen_context_paths.insert(entry.context_path.clone()) {
                        push_unique_warning(
                            &mut warnings,
                            "Duplicate linked context entry in registry was ignored.".to_string(),
                        );
                        continue;
                    }

                    match validate_linked_context(&entry.context_path) {
                        Ok(validated) if validated.source_id == entry.source_id => {
                            let acc =
                                builders
                                    .entry(validated.source_id.clone())
                                    .or_insert_with(|| {
                                        SourceSpecAccumulator::new(
                                            validated.source_id.clone(),
                                            false,
                                            validated.registry_dir.clone(),
                                            validated.plans_dir.clone(),
                                            validated.authority_dir.clone(),
                                            validated.sessions_dir.clone(),
                                        )
                                    });
                            if acc.registry_dir != validated.registry_dir
                                || acc.plans_dir != validated.plans_dir
                                || acc.authority_dir != validated.authority_dir
                                || acc.sessions_dir != validated.sessions_dir
                            {
                                push_unique_warning(
                                    &mut warnings,
                                    format!(
                                        "Linked Threadlane source {} has conflicting durable store paths and was ignored.",
                                        entry.source_id
                                    ),
                                );
                            } else {
                                acc.push_runtime_state_dir(validated.state_dir);
                                acc.project_roots.insert(validated.project_root);
                                acc.binding_ids.insert(validated.binding_id);
                                acc.thread_uuids.insert(validated.thread_uuid);
                            }
                        }
                        Ok(_) | Err(_) => {
                            push_unique_warning(
                                &mut warnings,
                                format!(
                                    "Linked Threadlane source {} is unavailable because its context or stores failed validation.",
                                    entry.source_id
                                ),
                            );
                            unavailable_sources
                                .entry(entry.source_id.clone())
                                .or_insert_with(|| ObservationSource {
                                    id: entry.source_id,
                                    label: LINKED_SOURCE_LABEL.to_string(),
                                    project_path: None,
                                    availability: ObservationSourceAvailability::Unavailable,
                                    message: Some(
                                        "Linked Threadlane context or durable stores are unavailable."
                                            .to_string(),
                                    ),
                                });
                        }
                    }
                }
            }
            Err(registry_warning) => {
                push_unique_warning(&mut warnings, registry_warning);
            }
        }

        let mut specs = Vec::with_capacity(builders.len());
        let mut sources = Vec::with_capacity(builders.len() + unavailable_sources.len());

        for (source_id, acc) in builders {
            unavailable_sources.remove(&source_id);
            let spec = acc.build();
            sources.push(spec.source.clone());
            specs.push(spec);
        }

        for (_, unavailable_source) in unavailable_sources {
            sources.push(unavailable_source);
        }

        sources.sort_by(|a, b| a.id.cmp(&b.id));
        specs.sort_by(|a, b| a.source.id.cmp(&b.source.id));

        SourceInventory {
            sources,
            specs,
            warnings,
        }
    }

    #[cfg(unix)]
    pub(crate) fn link_context(
        &self,
        context_path: &Path,
    ) -> Result<ObservationSource, ObservationSourceError> {
        let validated = validate_linked_context(context_path)?;
        let config = self.config();

        persist_linked_context_entry(
            &config.app_data_dir,
            &validated.context_path,
            &validated.source_id,
        )?;

        let inventory = self.inventory();
        if let Some(source) = inventory
            .sources
            .into_iter()
            .find(|source| source.id == validated.source_id)
        {
            return Ok(source);
        }

        Ok(ObservationSource {
            id: validated.source_id,
            label: LINKED_SOURCE_LABEL.to_string(),
            project_path: Some(validated.project_root.to_string_lossy().into_owned()),
            availability: ObservationSourceAvailability::Available,
            message: None,
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn inventory(&self) -> SourceInventory {
        SourceInventory {
            sources: vec![ObservationSource {
                id: UNSUPPORTED_PLATFORM_SOURCE_ID.to_string(),
                label: LOCAL_SOURCE_LABEL.to_string(),
                project_path: None,
                availability: ObservationSourceAvailability::Unavailable,
                message: Some(UNSUPPORTED_PLATFORM_MESSAGE.to_string()),
            }],
            specs: Vec::new(),
            warnings: vec![UNSUPPORTED_PLATFORM_MESSAGE.to_string()],
        }
    }

    #[cfg(not(unix))]
    pub(crate) fn link_context(
        &self,
        _context_path: &Path,
    ) -> Result<ObservationSource, ObservationSourceError> {
        Err(ObservationSourceError::UnsupportedPlatform)
    }
}

#[cfg(unix)]
struct SourceSpecAccumulator {
    source_id: String,
    is_local_default: bool,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    runtime_state_dirs: Vec<PathBuf>,
    project_roots: BTreeSet<PathBuf>,
    binding_ids: BTreeSet<String>,
    thread_uuids: BTreeSet<String>,
}

#[cfg(unix)]
impl SourceSpecAccumulator {
    fn new(
        source_id: String,
        is_local_default: bool,
        registry_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
    ) -> Self {
        Self {
            source_id,
            is_local_default,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            runtime_state_dirs: Vec::new(),
            project_roots: BTreeSet::new(),
            binding_ids: BTreeSet::new(),
            thread_uuids: BTreeSet::new(),
        }
    }

    fn push_runtime_state_dir(&mut self, dir: PathBuf) {
        if !self.runtime_state_dirs.contains(&dir) {
            self.runtime_state_dirs.push(dir);
        }
    }

    fn build(self) -> SourceSpec {
        let project_root_hint = unambiguous_element(&self.project_roots);
        let binding_id_hint = unambiguous_element(&self.binding_ids);
        let thread_uuid_hint = unambiguous_element(&self.thread_uuids);

        let label = if self.is_local_default {
            LOCAL_SOURCE_LABEL.to_string()
        } else {
            LINKED_SOURCE_LABEL.to_string()
        };

        let project_path = project_root_hint
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());

        let source = ObservationSource {
            id: self.source_id,
            label,
            project_path,
            availability: ObservationSourceAvailability::Available,
            message: None,
        };

        SourceSpec {
            source,
            registry_dir: self.registry_dir,
            plans_dir: self.plans_dir,
            authority_dir: self.authority_dir,
            sessions_dir: self.sessions_dir,
            runtime_state_dirs: self.runtime_state_dirs,
            project_root_hint,
            binding_id_hint,
            thread_uuid_hint,
        }
    }
}

#[cfg(unix)]
fn unambiguous_element<T: Clone>(set: &BTreeSet<T>) -> Option<T> {
    if set.len() == 1 {
        set.iter().next().cloned()
    } else {
        None
    }
}

#[cfg(unix)]
fn push_unique_warning(warnings: &mut Vec<String>, warning: String) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

#[cfg(unix)]
struct ValidatedDefaultSource {
    source_id: String,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    runtime_state_dir: Option<PathBuf>,
    runtime_warning: Option<String>,
}

#[cfg(unix)]
struct UnavailableDefaultSource {
    source_id: String,
    warning: String,
}

#[cfg(unix)]
fn validate_default_source(
    config: &ObservationSourceConfig,
) -> Result<ValidatedDefaultSource, UnavailableDefaultSource> {
    let fallback_id = compute_fallback_source_id(
        &config.plans_dir,
        &config.authority_dir,
        &config.sessions_dir,
    );

    let registry_dir =
        validate_canonical_private_store_dir(&config.registry_dir).map_err(|_| {
            UnavailableDefaultSource {
                source_id: fallback_id.clone(),
                warning: "Local Threadlane durable stores failed private directory validation."
                    .to_string(),
            }
        })?;
    let plans_dir = validate_canonical_private_store_dir(&config.plans_dir).map_err(|_| {
        UnavailableDefaultSource {
            source_id: fallback_id.clone(),
            warning: "Local Threadlane durable stores failed private directory validation."
                .to_string(),
        }
    })?;
    let authority_dir =
        validate_canonical_private_store_dir(&config.authority_dir).map_err(|_| {
            UnavailableDefaultSource {
                source_id: fallback_id.clone(),
                warning: "Local Threadlane durable stores failed private directory validation."
                    .to_string(),
            }
        })?;
    let sessions_dir =
        validate_canonical_private_store_dir(&config.sessions_dir).map_err(|_| {
            UnavailableDefaultSource {
                source_id: fallback_id.clone(),
                warning: "Local Threadlane durable stores failed private directory validation."
                    .to_string(),
            }
        })?;

    let durable_dirs = [&registry_dir, &plans_dir, &authority_dir, &sessions_dir];
    if !are_paths_pairwise_disjoint(&durable_dirs) {
        return Err(UnavailableDefaultSource {
            source_id: compute_stable_source_id(&plans_dir, &authority_dir, &sessions_dir),
            warning: "Local Threadlane durable store directories overlap.".to_string(),
        });
    }

    let source_id = compute_observation_source_id(&plans_dir, &authority_dir, &sessions_dir)
        .map_err(|_| UnavailableDefaultSource {
            source_id: compute_stable_source_id_from_canonical_paths(
                &plans_dir,
                &authority_dir,
                &sessions_dir,
            ),
            warning: "Local Threadlane durable stores failed private directory validation."
                .to_string(),
        })?;

    let mut runtime_state_dir = None;
    let mut runtime_warning = None;
    if let Some(ref configured_rt) = config.runtime_state_dir {
        if !is_lexically_normalized_absolute_path(configured_rt) {
            runtime_warning = Some(
                "Optional local runtime state directory path is not canonical and was ignored."
                    .to_string(),
            );
        } else {
            match ensure_no_symlink_components(configured_rt) {
                Err(PathComponentError::NotFound) => {}
                Err(PathComponentError::SymlinkOrInvalid) => {
                    runtime_warning = Some(
                        "Optional local runtime state directory contains a symlink or is inaccessible and was ignored."
                            .to_string(),
                    );
                }
                Ok(()) => match validate_canonical_private_store_dir(configured_rt) {
                    Ok(canonical_rt) => {
                        if durable_dirs
                            .iter()
                            .any(|durable| paths_overlap(&canonical_rt, durable))
                        {
                            runtime_warning = Some(
                                "Optional local runtime state directory overlaps durable stores and was ignored."
                                    .to_string(),
                            );
                        } else {
                            runtime_state_dir = Some(canonical_rt);
                        }
                    }
                    Err(_) => {
                        runtime_warning = Some(
                            "Optional local runtime state directory failed private directory validation and was ignored."
                                .to_string(),
                        );
                    }
                },
            }
        }
    }

    Ok(ValidatedDefaultSource {
        source_id,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        runtime_state_dir,
        runtime_warning,
    })
}

#[cfg(unix)]
struct ValidatedLinkedContext {
    context_path: PathBuf,
    source_id: String,
    project_root: PathBuf,
    binding_id: String,
    thread_uuid: String,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
}

#[cfg(unix)]
fn validate_linked_context(
    context_path: &Path,
) -> Result<ValidatedLinkedContext, ObservationSourceError> {
    let (canonical_context_path, raw_bytes) = validate_and_read_context_file(context_path)?;
    let parsed: RawPrivateContext = deserialize_private_context(&raw_bytes)?;

    if parsed.schema_version != 1 {
        return Err(ObservationSourceError::InvalidSchemaVersion);
    }
    if parsed.route != EXPECTED_CONTEXT_ROUTE {
        return Err(ObservationSourceError::InvalidContextRoute);
    }
    if parsed.native_per_call_identity_verified {
        return Err(ObservationSourceError::InvalidContextIdentityFlag);
    }

    validate_binding_id_syntax(&parsed.binding_id)?;
    let canonical_thread_uuid = validate_non_nil_thread_uuid(&parsed.thread_uuid)?;
    let canonical_project_root = validate_canonical_project_root(&parsed.project_root)?;

    let state_dir = validate_canonical_private_store_dir(&parsed.stores.state_dir)?;
    let registry_dir = validate_canonical_private_store_dir(&parsed.stores.registry_dir)?;
    let plans_dir = validate_canonical_private_store_dir(&parsed.stores.plans_dir)?;
    let authority_dir = validate_canonical_private_store_dir(&parsed.stores.authority_dir)?;
    let sessions_dir = validate_canonical_private_store_dir(&parsed.stores.sessions_dir)?;

    let all_stores = [
        &state_dir,
        &registry_dir,
        &plans_dir,
        &authority_dir,
        &sessions_dir,
    ];
    if !are_paths_pairwise_disjoint(&all_stores) {
        return Err(ObservationSourceError::OverlappingStoreDirectories);
    }
    if all_stores
        .iter()
        .any(|store| paths_overlap(&canonical_project_root, store))
    {
        return Err(ObservationSourceError::ProjectRootOverlapsStore);
    }

    let source_id = compute_observation_source_id(&plans_dir, &authority_dir, &sessions_dir)
        .map_err(|_| ObservationSourceError::InvalidStoreDirectory)?;

    Ok(ValidatedLinkedContext {
        context_path: canonical_context_path,
        source_id,
        project_root: canonical_project_root,
        binding_id: parsed.binding_id,
        thread_uuid: canonical_thread_uuid,
        state_dir,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
    })
}

#[cfg(unix)]
fn validate_and_read_context_file(
    path: &Path,
) -> Result<(PathBuf, Vec<u8>), ObservationSourceError> {
    if !is_lexically_normalized_absolute_path(path) {
        return Err(ObservationSourceError::InvalidContextPath);
    }
    let parent = path
        .parent()
        .ok_or(ObservationSourceError::InvalidContextPath)?;
    if parent.as_os_str().is_empty() || !is_lexically_normalized_absolute_path(parent) {
        return Err(ObservationSourceError::InvalidContextPath);
    }

    ensure_no_symlink_components(path).map_err(|err| match err {
        PathComponentError::NotFound => ObservationSourceError::InaccessibleContextFile,
        PathComponentError::SymlinkOrInvalid => ObservationSourceError::InvalidContextPath,
    })?;

    validate_existing_private_dir_readonly(parent)
        .map_err(|_| ObservationSourceError::InaccessibleContextFile)?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| ObservationSourceError::InaccessibleContextFile)?;
    if canonical_parent != parent {
        return Err(ObservationSourceError::InvalidContextPath);
    }

    let validated_meta = validate_existing_private_file_readonly(path, MAX_CONTEXT_FILE_BYTES)
        .map_err(|_| ObservationSourceError::InaccessibleContextFile)?;
    if validated_meta.len() > MAX_CONTEXT_FILE_BYTES {
        return Err(ObservationSourceError::InaccessibleContextFile);
    }

    let canonical_path =
        fs::canonicalize(path).map_err(|_| ObservationSourceError::InaccessibleContextFile)?;
    if canonical_path != path {
        return Err(ObservationSourceError::InvalidContextPath);
    }

    let bytes = read_validated_private_file_bytes(path, &validated_meta, MAX_CONTEXT_FILE_BYTES)
        .map_err(|_| ObservationSourceError::InaccessibleContextFile)?;

    Ok((canonical_path, bytes))
}

#[cfg(unix)]
fn read_validated_private_file_bytes(
    path: &Path,
    expected_meta: &fs::Metadata,
    max_bytes: u64,
) -> Result<Vec<u8>, ()> {
    let lstat = fs::symlink_metadata(path).map_err(|_| ())?;
    if lstat.file_type().is_symlink()
        || !lstat.file_type().is_file()
        || (lstat.mode() & 0o7777) != 0o600
        || lstat.len() > max_bytes
        || lstat.dev() != expected_meta.dev()
        || lstat.ino() != expected_meta.ino()
    {
        return Err(());
    }

    let file = File::open(path).map_err(|_| ())?;
    let fstat = file.metadata().map_err(|_| ())?;
    if fstat.file_type().is_symlink()
        || !fstat.file_type().is_file()
        || (fstat.mode() & 0o7777) != 0o600
        || fstat.len() > max_bytes
        || fstat.dev() != lstat.dev()
        || fstat.ino() != lstat.ino()
    {
        return Err(());
    }

    let mut buf = Vec::with_capacity(fstat.len() as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut buf)
        .map_err(|_| ())?;
    if (buf.len() as u64) > max_bytes {
        return Err(());
    }

    Ok(buf)
}

#[cfg(unix)]
fn validate_canonical_private_store_dir(dir: &Path) -> Result<PathBuf, ObservationSourceError> {
    if !is_lexically_normalized_absolute_path(dir) {
        return Err(ObservationSourceError::InvalidStoreDirectory);
    }
    ensure_no_symlink_components(dir).map_err(|_| ObservationSourceError::InvalidStoreDirectory)?;
    validate_existing_private_dir_readonly(dir)
        .map_err(|_| ObservationSourceError::InvalidStoreDirectory)?;
    let canonical =
        fs::canonicalize(dir).map_err(|_| ObservationSourceError::InvalidStoreDirectory)?;
    if canonical != dir {
        return Err(ObservationSourceError::InvalidStoreDirectory);
    }
    Ok(canonical)
}

#[cfg(unix)]
fn validate_canonical_project_root(project_root: &Path) -> Result<PathBuf, ObservationSourceError> {
    if !is_lexically_normalized_absolute_path(project_root) {
        return Err(ObservationSourceError::InvalidProjectRoot);
    }
    ensure_no_symlink_components(project_root)
        .map_err(|_| ObservationSourceError::InvalidProjectRoot)?;
    let meta = fs::symlink_metadata(project_root)
        .map_err(|_| ObservationSourceError::InvalidProjectRoot)?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ObservationSourceError::InvalidProjectRoot);
    }
    let canonical =
        fs::canonicalize(project_root).map_err(|_| ObservationSourceError::InvalidProjectRoot)?;
    if canonical != project_root {
        return Err(ObservationSourceError::InvalidProjectRoot);
    }
    Ok(canonical)
}

#[cfg(unix)]
fn validate_existing_app_data_dir(app_data_dir: &Path) -> Result<PathBuf, ObservationSourceError> {
    if !is_lexically_normalized_absolute_path(app_data_dir) {
        return Err(ObservationSourceError::InvalidAppDataDirectory);
    }
    ensure_no_symlink_components(app_data_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
    let meta = fs::symlink_metadata(app_data_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ObservationSourceError::InvalidAppDataDirectory);
    }
    let canonical = fs::canonicalize(app_data_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
    if canonical != app_data_dir {
        return Err(ObservationSourceError::InvalidAppDataDirectory);
    }
    Ok(canonical)
}

#[cfg(unix)]
fn ensure_observations_dir(observations_dir: &Path) -> Result<(), ObservationSourceError> {
    match fs::symlink_metadata(observations_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            match DirBuilder::new().mode(0o700).create(observations_dir) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(ObservationSourceError::InvalidAppDataDirectory),
            }
        }
        Err(_) => return Err(ObservationSourceError::InvalidAppDataDirectory),
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(ObservationSourceError::InvalidAppDataDirectory);
            }
        }
    }

    validate_existing_private_dir_readonly(observations_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
    let canonical_observations_dir = fs::canonicalize(observations_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
    if canonical_observations_dir != observations_dir {
        return Err(ObservationSourceError::InvalidAppDataDirectory);
    }
    Ok(())
}

#[cfg(unix)]
fn acquire_linked_registry_lock(observations_dir: &Path) -> Result<File, ObservationSourceError> {
    // The lock file is intentionally persistent so every process locks the same inode.
    let lock_path = observations_dir.join(LINKED_CONTEXTS_LOCK_FILE_NAME);

    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&lock_path)
    {
        Ok(file) => {
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
            file
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?,
        Err(_) => return Err(ObservationSourceError::LinkRegistryWriteFailed),
    };

    validate_linked_registry_lock_file(&lock_path, &file)?;
    file.lock()
        .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
    validate_linked_registry_lock_file(&lock_path, &file)?;

    Ok(file)
}

#[cfg(unix)]
fn validate_linked_registry_lock_file(
    lock_path: &Path,
    file: &File,
) -> Result<(), ObservationSourceError> {
    let validated = validate_existing_private_file_readonly(lock_path, 0)
        .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
    let handle = file
        .metadata()
        .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
    if handle.file_type().is_symlink()
        || !handle.file_type().is_file()
        || (handle.mode() & 0o7777) != 0o600
        || (validated.mode() & 0o7777) != 0o600
        || validated.dev() != handle.dev()
        || validated.ino() != handle.ino()
        || validated.uid() != handle.uid()
        || validated.gid() != handle.gid()
    {
        return Err(ObservationSourceError::InvalidLinkRegistry);
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
enum PathComponentError {
    NotFound,
    SymlinkOrInvalid,
}

#[cfg(unix)]
fn is_lexically_normalized_absolute_path(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    if !path.is_absolute() {
        return false;
    }
    let bytes = path.as_os_str().as_bytes();
    if bytes.len() <= 1 || bytes[0] != b'/' || bytes.ends_with(b"/") {
        return false;
    }
    for segment in bytes[1..].split(|&b| b == b'/') {
        if segment.is_empty() || segment == b"." || segment == b".." || segment.contains(&0) {
            return false;
        }
    }
    let mut has_normal = false;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(_) => has_normal = true,
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => return false,
        }
    }
    has_normal
}

#[cfg(unix)]
fn ensure_no_symlink_components(path: &Path) -> Result<(), PathComponentError> {
    let mut current = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => continue,
            Component::Normal(segment) => {
                current.push(segment);
                let meta = fs::symlink_metadata(&current).map_err(|err| {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        PathComponentError::NotFound
                    } else {
                        PathComponentError::SymlinkOrInvalid
                    }
                })?;
                if meta.file_type().is_symlink() {
                    return Err(PathComponentError::SymlinkOrInvalid);
                }
            }
            _ => return Err(PathComponentError::SymlinkOrInvalid),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

#[cfg(unix)]
fn are_paths_pairwise_disjoint(paths: &[&PathBuf]) -> bool {
    for i in 0..paths.len() {
        for j in (i + 1)..paths.len() {
            if paths_overlap(paths[i], paths[j]) {
                return false;
            }
        }
    }
    true
}

#[cfg(unix)]
fn validate_binding_id_syntax(binding_id: &str) -> Result<(), ObservationSourceError> {
    let bytes = binding_id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_BINDING_ID_BYTES {
        return Err(ObservationSourceError::InvalidBindingId);
    }
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(ObservationSourceError::InvalidBindingId);
    }
    if !bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(ObservationSourceError::InvalidBindingId);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_non_nil_thread_uuid(raw: &str) -> Result<String, ObservationSourceError> {
    if raw != raw.trim() {
        return Err(ObservationSourceError::InvalidThreadUuid);
    }
    let canonical = validate_canonical_thread_uuid("thread_uuid", raw)
        .map_err(|_| ObservationSourceError::InvalidThreadUuid)?;
    if canonical != raw || canonical == NIL_UUID {
        return Err(ObservationSourceError::InvalidThreadUuid);
    }
    Ok(canonical)
}

#[cfg(unix)]
fn compute_stable_source_id(plans_dir: &Path, authority_dir: &Path, sessions_dir: &Path) -> String {
    compute_observation_source_id(plans_dir, authority_dir, sessions_dir).unwrap_or_else(|_| {
        compute_stable_source_id_from_canonical_paths(plans_dir, authority_dir, sessions_dir)
    })
}

#[cfg(unix)]
fn compute_stable_source_id_from_canonical_paths(
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> String {
    use std::os::unix::ffi::OsStrExt;

    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-observation-source-v1\0");
    for (label, path) in [
        ("plans_dir", plans_dir),
        ("authority_dir", authority_dir),
        ("sessions_dir", sessions_dir),
    ] {
        let raw = path.as_os_str().as_bytes();
        hasher.update(label.as_bytes());
        hasher.update(b"\0");
        hasher.update((raw.len() as u64).to_be_bytes());
        hasher.update(raw);
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    format!("src_{}", encode_hex_lower(&digest))
}

#[cfg(unix)]
fn compute_fallback_source_id(
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
) -> String {
    let canonical_plans = fs::canonicalize(plans_dir).unwrap_or_else(|_| plans_dir.to_path_buf());
    let canonical_authority =
        fs::canonicalize(authority_dir).unwrap_or_else(|_| authority_dir.to_path_buf());
    let canonical_sessions =
        fs::canonicalize(sessions_dir).unwrap_or_else(|_| sessions_dir.to_path_buf());
    compute_stable_source_id_from_canonical_paths(
        &canonical_plans,
        &canonical_authority,
        &canonical_sessions,
    )
}

#[cfg(unix)]
fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(unix)]
fn is_valid_source_id(id: &str) -> bool {
    let Some(hex) = id.strip_prefix("src_") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PersistedLinkedContextRegistry {
    schema_version: u32,
    links: Vec<PersistedLinkedContextEntry>,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PersistedLinkedContextEntry {
    source_id: String,
    context_path: PathBuf,
}

#[cfg(unix)]
impl<'de> Deserialize<'de> for PersistedLinkedContextRegistry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RegistryVisitor;

        impl<'de> Visitor<'de> for RegistryVisitor {
            type Value = PersistedLinkedContextRegistry;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a persisted linked context registry object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut schema_version: Option<u32> = None;
                let mut links: Option<Vec<PersistedLinkedContextEntry>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate key in linked registry"));
                    }
                    match key.as_str() {
                        "schema_version" => {
                            schema_version = Some(map.next_value::<u32>()?);
                        }
                        "links" => {
                            links = Some(map.next_value::<Vec<PersistedLinkedContextEntry>>()?);
                        }
                        _ => return Err(de::Error::custom("unknown key in linked registry")),
                    }
                }

                Ok(PersistedLinkedContextRegistry {
                    schema_version: schema_version
                        .ok_or_else(|| de::Error::custom("missing schema_version"))?,
                    links: links.ok_or_else(|| de::Error::custom("missing links"))?,
                })
            }
        }

        deserializer.deserialize_map(RegistryVisitor)
    }
}

#[cfg(unix)]
impl<'de> Deserialize<'de> for PersistedLinkedContextEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = PersistedLinkedContextEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a persisted linked context entry object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut source_id: Option<String> = None;
                let mut context_path: Option<PathBuf> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate key in linked entry"));
                    }
                    match key.as_str() {
                        "source_id" => {
                            source_id = Some(map.next_value::<String>()?);
                        }
                        "context_path" => {
                            context_path = Some(map.next_value::<PathBuf>()?);
                        }
                        _ => return Err(de::Error::custom("unknown key in linked entry")),
                    }
                }

                Ok(PersistedLinkedContextEntry {
                    source_id: source_id.ok_or_else(|| de::Error::custom("missing source_id"))?,
                    context_path: context_path
                        .ok_or_else(|| de::Error::custom("missing context_path"))?,
                })
            }
        }

        deserializer.deserialize_map(EntryVisitor)
    }
}

#[cfg(unix)]
fn read_linked_context_entries_readonly(
    app_data_dir: &Path,
) -> Result<Vec<PersistedLinkedContextEntry>, String> {
    if !is_lexically_normalized_absolute_path(app_data_dir) {
        return Err("Application data directory path is not canonical.".to_string());
    }

    match ensure_no_symlink_components(app_data_dir) {
        Err(PathComponentError::NotFound) => return Ok(Vec::new()),
        Err(PathComponentError::SymlinkOrInvalid) => {
            return Err("Application data directory contains a symlink component.".to_string());
        }
        Ok(()) => {}
    }

    match fs::symlink_metadata(app_data_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("Application data directory is inaccessible.".to_string()),
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err("Application data directory is invalid.".to_string());
            }
        }
    }

    let observations_dir = app_data_dir.join(OBSERVATIONS_DIR_NAME);
    match fs::symlink_metadata(&observations_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("Linked observations directory is inaccessible.".to_string()),
        Ok(_) => {}
    }

    if validate_existing_private_dir_readonly(&observations_dir).is_err() {
        return Err(
            "Linked observations directory failed private directory validation.".to_string(),
        );
    }
    if fs::canonicalize(&observations_dir).ok().as_deref() != Some(observations_dir.as_path()) {
        return Err("Linked observations directory path is not canonical.".to_string());
    }

    let registry_path = observations_dir.join(LINKED_CONTEXTS_FILE_NAME);
    match fs::symlink_metadata(&registry_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("Linked context registry is inaccessible.".to_string()),
        Ok(_) => {}
    }

    let meta =
        validate_existing_private_file_readonly(&registry_path, MAX_LINKED_CONTEXT_REGISTRY_BYTES)
            .map_err(|_| "Linked context registry failed private file validation.".to_string())?;

    let bytes =
        read_validated_private_file_bytes(&registry_path, &meta, MAX_LINKED_CONTEXT_REGISTRY_BYTES)
            .map_err(|_| "Linked context registry could not be read safely.".to_string())?;

    parse_and_validate_registry_bytes(&bytes)
        .map_err(|_| "Linked context registry is malformed.".to_string())
}

#[cfg(unix)]
fn parse_and_validate_registry_bytes(
    bytes: &[u8],
) -> Result<Vec<PersistedLinkedContextEntry>, ObservationSourceError> {
    let registry: PersistedLinkedContextRegistry =
        serde_json::from_slice(bytes).map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
    if registry.schema_version != 1 || registry.links.len() > MAX_LINKED_CONTEXTS {
        return Err(ObservationSourceError::InvalidLinkRegistry);
    }
    for entry in &registry.links {
        if !is_valid_source_id(&entry.source_id)
            || !is_lexically_normalized_absolute_path(&entry.context_path)
        {
            return Err(ObservationSourceError::InvalidLinkRegistry);
        }
    }
    Ok(registry.links)
}

#[cfg(unix)]
fn persist_linked_context_entry(
    app_data_dir: &Path,
    canonical_context_path: &Path,
    source_id: &str,
) -> Result<(), ObservationSourceError> {
    let canonical_app_data_dir = validate_existing_app_data_dir(app_data_dir)?;
    let observations_dir = canonical_app_data_dir.join(OBSERVATIONS_DIR_NAME);
    ensure_observations_dir(&observations_dir)?;

    // Hold the exclusive lock through registry read-modify-write, rename, and directory sync.
    let _registry_lock = acquire_linked_registry_lock(&observations_dir)?;

    let registry_path = observations_dir.join(LINKED_CONTEXTS_FILE_NAME);
    let mut links = match fs::symlink_metadata(&registry_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(_) => return Err(ObservationSourceError::InvalidLinkRegistry),
        Ok(_) => {
            let meta = validate_existing_private_file_readonly(
                &registry_path,
                MAX_LINKED_CONTEXT_REGISTRY_BYTES,
            )
            .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
            let bytes = read_validated_private_file_bytes(
                &registry_path,
                &meta,
                MAX_LINKED_CONTEXT_REGISTRY_BYTES,
            )
            .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
            parse_and_validate_registry_bytes(&bytes)?
        }
    };

    let mut deduplicated: Vec<PersistedLinkedContextEntry> = Vec::with_capacity(links.len() + 1);
    let mut updated_existing = false;
    for mut entry in links.drain(..) {
        if deduplicated
            .iter()
            .any(|existing| existing.context_path == entry.context_path)
        {
            continue;
        }
        if entry.context_path == canonical_context_path {
            entry.source_id = source_id.to_string();
            updated_existing = true;
        }
        deduplicated.push(entry);
    }

    if !updated_existing {
        if deduplicated.len() >= MAX_LINKED_CONTEXTS {
            return Err(ObservationSourceError::LinkRegistryLimitReached);
        }
        deduplicated.push(PersistedLinkedContextEntry {
            source_id: source_id.to_string(),
            context_path: canonical_context_path.to_path_buf(),
        });
    }

    let payload = PersistedLinkedContextRegistry {
        schema_version: 1,
        links: deduplicated,
    };
    let serialized = serde_json::to_vec_pretty(&payload)
        .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
    if (serialized.len() as u64) > MAX_LINKED_CONTEXT_REGISTRY_BYTES {
        return Err(ObservationSourceError::LinkRegistryLimitReached);
    }

    write_private_registry_atomically(&observations_dir, &registry_path, &serialized)
}

#[cfg(unix)]
fn write_private_registry_atomically(
    observations_dir: &Path,
    registry_path: &Path,
    serialized: &[u8],
) -> Result<(), ObservationSourceError> {
    validate_existing_private_dir_readonly(observations_dir)
        .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;

    let temp_path = unique_registry_temp_path(observations_dir);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp_path)
        .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;

    let write_result = (|| -> Result<(), ObservationSourceError> {
        let meta = temp_file
            .metadata()
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        if meta.file_type().is_symlink()
            || !meta.file_type().is_file()
            || (meta.mode() & 0o7777) != 0o600
        {
            return Err(ObservationSourceError::LinkRegistryWriteFailed);
        }

        temp_file
            .write_all(serialized)
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        temp_file
            .flush()
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        temp_file
            .sync_all()
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        drop(temp_file);

        validate_existing_private_dir_readonly(observations_dir)
            .map_err(|_| ObservationSourceError::InvalidAppDataDirectory)?;
        match fs::symlink_metadata(registry_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ObservationSourceError::InvalidLinkRegistry),
            Ok(_) => {
                validate_existing_private_file_readonly(
                    registry_path,
                    MAX_LINKED_CONTEXT_REGISTRY_BYTES,
                )
                .map_err(|_| ObservationSourceError::InvalidLinkRegistry)?;
            }
        }

        fs::rename(&temp_path, registry_path)
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;

        let parent_dir = File::open(observations_dir)
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        parent_dir
            .sync_all()
            .map_err(|_| ObservationSourceError::LinkRegistryWriteFailed)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

#[cfg(unix)]
fn unique_registry_temp_path(observations_dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let seq = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    observations_dir.join(format!(
        ".{LINKED_CONTEXTS_FILE_NAME}.tmp.{pid}.{seq}.{nanos}"
    ))
}

#[cfg(unix)]
#[derive(Debug)]
struct RawPrivateContext {
    schema_version: i64,
    project_root: PathBuf,
    binding_id: String,
    thread_uuid: String,
    route: String,
    native_per_call_identity_verified: bool,
    stores: RawPrivateContextStores,
}

#[cfg(unix)]
#[derive(Debug)]
struct RawPrivateContextStores {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
}

#[cfg(unix)]
const FORBIDDEN_ALIAS_MARKER: &str = "__threadlane_forbidden_top_level_alias__";

#[cfg(unix)]
fn deserialize_private_context(bytes: &[u8]) -> Result<RawPrivateContext, ObservationSourceError> {
    serde_json::from_slice::<RawPrivateContext>(bytes).map_err(|err| {
        if err.to_string().contains(FORBIDDEN_ALIAS_MARKER) {
            ObservationSourceError::ForbiddenContextAlias
        } else {
            ObservationSourceError::InvalidContextFormat
        }
    })
}

#[cfg(unix)]
struct NoDuplicateIgnoredValue;

#[cfg(unix)]
impl<'de> Deserialize<'de> for NoDuplicateIgnoredValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NoDuplicateIgnoredVisitor;

        impl<'de> Visitor<'de> for NoDuplicateIgnoredVisitor {
            type Value = NoDuplicateIgnoredValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("any JSON value without duplicate object keys")
            }

            fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_i64<E>(self, _v: i64) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_u64<E>(self, _v: u64) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_f64<E>(self, _v: f64) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_str<E>(self, _v: &str) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_string<E>(self, _v: String) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                NoDuplicateIgnoredValue::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                while seq.next_element::<NoDuplicateIgnoredValue>()?.is_some() {}
                Ok(NoDuplicateIgnoredValue)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen = BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key) {
                        return Err(de::Error::custom("duplicate key in nested object"));
                    }
                    let _ = map.next_value::<NoDuplicateIgnoredValue>()?;
                }
                Ok(NoDuplicateIgnoredValue)
            }
        }

        deserializer.deserialize_any(NoDuplicateIgnoredVisitor)
    }
}

#[cfg(unix)]
impl<'de> Deserialize<'de> for RawPrivateContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawPrivateContextVisitor;

        impl<'de> Visitor<'de> for RawPrivateContextVisitor {
            type Value = RawPrivateContext;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Threadlane private context JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut schema_version: Option<i64> = None;
                let mut project_root: Option<PathBuf> = None;
                let mut binding_id: Option<String> = None;
                let mut thread_uuid: Option<String> = None;
                let mut route: Option<String> = None;
                let mut native_per_call_identity_verified: Option<bool> = None;
                let mut stores: Option<RawPrivateContextStores> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate top-level field"));
                    }

                    match key.as_str() {
                        "schema_version" | "version" => {
                            if schema_version.is_some() {
                                return Err(de::Error::custom(
                                    "duplicate schema_version and version fields",
                                ));
                            }
                            schema_version = Some(map.next_value::<i64>()?);
                        }
                        "project_root" => {
                            project_root = Some(map.next_value::<PathBuf>()?);
                        }
                        "binding_id" => {
                            binding_id = Some(map.next_value::<String>()?);
                        }
                        "thread_uuid" => {
                            thread_uuid = Some(map.next_value::<String>()?);
                        }
                        "route" => {
                            route = Some(map.next_value::<String>()?);
                        }
                        "native_per_call_identity_verified" => {
                            native_per_call_identity_verified = Some(map.next_value::<bool>()?);
                        }
                        "stores" => {
                            stores = Some(map.next_value::<RawPrivateContextStores>()?);
                        }
                        "cli_package" => {
                            let _ = map.next_value::<NoDuplicateIgnoredValue>()?;
                        }
                        "state_dir" | "registry_dir" | "plans_dir" | "authority_dir"
                        | "sessions_dir" | "binding" | "thread" => {
                            return Err(de::Error::custom(FORBIDDEN_ALIAS_MARKER));
                        }
                        _ => {
                            let _ = map.next_value::<NoDuplicateIgnoredValue>()?;
                        }
                    }
                }

                Ok(RawPrivateContext {
                    schema_version: schema_version
                        .ok_or_else(|| de::Error::custom("missing schema_version"))?,
                    project_root: project_root
                        .ok_or_else(|| de::Error::custom("missing project_root"))?,
                    binding_id: binding_id
                        .ok_or_else(|| de::Error::custom("missing binding_id"))?,
                    thread_uuid: thread_uuid
                        .ok_or_else(|| de::Error::custom("missing thread_uuid"))?,
                    route: route.ok_or_else(|| de::Error::custom("missing route"))?,
                    native_per_call_identity_verified: native_per_call_identity_verified
                        .ok_or_else(|| {
                            de::Error::custom("missing native_per_call_identity_verified")
                        })?,
                    stores: stores.ok_or_else(|| de::Error::custom("missing stores"))?,
                })
            }
        }

        deserializer.deserialize_map(RawPrivateContextVisitor)
    }
}

#[cfg(unix)]
impl<'de> Deserialize<'de> for RawPrivateContextStores {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawPrivateContextStoresVisitor;

        impl<'de> Visitor<'de> for RawPrivateContextStoresVisitor {
            type Value = RawPrivateContextStores;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Threadlane private context stores object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut state_dir: Option<PathBuf> = None;
                let mut registry_dir: Option<PathBuf> = None;
                let mut plans_dir: Option<PathBuf> = None;
                let mut authority_dir: Option<PathBuf> = None;
                let mut sessions_dir: Option<PathBuf> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate field in stores"));
                    }

                    match key.as_str() {
                        "state_dir" => {
                            state_dir = Some(map.next_value::<PathBuf>()?);
                        }
                        "registry_dir" => {
                            registry_dir = Some(map.next_value::<PathBuf>()?);
                        }
                        "plans_dir" => {
                            plans_dir = Some(map.next_value::<PathBuf>()?);
                        }
                        "authority_dir" => {
                            authority_dir = Some(map.next_value::<PathBuf>()?);
                        }
                        "sessions_dir" => {
                            sessions_dir = Some(map.next_value::<PathBuf>()?);
                        }
                        _ => {
                            return Err(de::Error::custom("unknown field in stores"));
                        }
                    }
                }

                Ok(RawPrivateContextStores {
                    state_dir: state_dir
                        .ok_or_else(|| de::Error::custom("missing stores.state_dir"))?,
                    registry_dir: registry_dir
                        .ok_or_else(|| de::Error::custom("missing stores.registry_dir"))?,
                    plans_dir: plans_dir
                        .ok_or_else(|| de::Error::custom("missing stores.plans_dir"))?,
                    authority_dir: authority_dir
                        .ok_or_else(|| de::Error::custom("missing stores.authority_dir"))?,
                    sessions_dir: sessions_dir
                        .ok_or_else(|| de::Error::custom("missing stores.sessions_dir"))?,
                })
            }
        }

        deserializer.deserialize_map(RawPrivateContextStoresVisitor)
    }
}

#[cfg(all(test, unix))]
#[path = "sources_tests.rs"]
mod sources_tests;

#[cfg(all(test, unix))]
mod native_source_id_unification_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn make_private_dir(root: &Path, name: &str) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).expect("create private store dir");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 store dir");
        dir
    }

    #[test]
    fn compute_stable_source_id_matches_native_producer_without_changing_legacy_results() {
        let base = fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
        let unique = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = base.join(format!(
            "threadlane-source-id-parity-{}-{unique}",
            std::process::id()
        ));
        if root.exists() {
            let _ = fs::remove_dir_all(&root);
        }
        fs::create_dir_all(&root).expect("create test root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod 0700 root");

        let registry_dir = make_private_dir(&root, "registry");
        let plans_dir = make_private_dir(&root, "plans-türkçe-α");
        let authority_dir = make_private_dir(&root, "authority store");
        let sessions_dir = make_private_dir(&root, "sessions");
        let app_data_dir = make_private_dir(&root, "app-data");

        let legacy_digest = compute_stable_source_id_from_canonical_paths(
            &plans_dir,
            &authority_dir,
            &sessions_dir,
        );
        let unified_stable = compute_stable_source_id(&plans_dir, &authority_dir, &sessions_dir);
        let native_id = compute_observation_source_id(&plans_dir, &authority_dir, &sessions_dir)
            .expect("native source id computation must succeed");

        assert_eq!(unified_stable, legacy_digest);
        assert_eq!(native_id, legacy_digest);
        assert!(is_valid_source_id(&native_id));

        let alias_plans = root.join("plans-link");
        symlink(&plans_dir, &alias_plans).expect("create plans symlink");
        let fallback_via_symlink =
            compute_fallback_source_id(&alias_plans, &authority_dir, &sessions_dir);
        assert_eq!(fallback_via_symlink, legacy_digest);

        let registry = ObservationSourceRegistry::new(ObservationSourceConfig {
            runtime_state_dir: None,
            registry_dir,
            plans_dir: plans_dir.clone(),
            authority_dir: authority_dir.clone(),
            sessions_dir: sessions_dir.clone(),
            app_data_dir,
        });
        let inventory = registry.inventory();
        assert_eq!(inventory.sources.len(), 1);
        assert_eq!(inventory.sources[0].id, native_id);
        assert_eq!(
            inventory.sources[0].availability,
            ObservationSourceAvailability::Available
        );

        let missing_plans = root.join("missing-plans");
        let missing_authority = root.join("missing-authority");
        let missing_sessions = root.join("missing-sessions");
        assert_eq!(
            compute_stable_source_id(&missing_plans, &missing_authority, &missing_sessions),
            compute_stable_source_id_from_canonical_paths(
                &missing_plans,
                &missing_authority,
                &missing_sessions,
            )
        );
        assert!(!missing_plans.exists());

        let _ = fs::remove_dir_all(&root);
    }
}

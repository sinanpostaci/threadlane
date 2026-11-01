use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Safe, user-facing error returned by [`crate::RegistryStore`] and [`crate::resolve_choice`].
///
/// `Display` messages are concise static English strings (plus safe numeric revisions for
/// [`RegistryError::StaleRevision`]) and never leak filesystem paths, raw file contents,
/// environment variables, or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum RegistryError {
    /// Caller supplied an empty, overlong, or malformed identifier, label, or revision.
    #[error("{0}")]
    InvalidInput(&'static str),

    /// A model, profile assignment, decision advisor, or default profile references a
    /// nonexistent, disabled, or incompatible entity.
    #[error("{0}")]
    InvalidReference(&'static str),

    /// A workflow profile contains more than one assignment for the same role and difficulty.
    #[error("{0}")]
    DuplicateAssignment(&'static str),

    /// A workflow assignment or decision advisor specifies or requires an effort level that is
    /// not supported by the target model.
    #[error("{0}")]
    UnsupportedEffort(&'static str),

    /// An entity cannot be deleted because another model or profile still references it.
    #[error("{0}")]
    ReferenceConflict(&'static str),

    /// No matching role/difficulty or role-default assignment exists in the selected profile.
    #[error("{0}")]
    MissingAssignment(&'static str),

    /// Requested tool, model, or workflow profile does not exist in the registry.
    #[error("{0}")]
    NotFound(&'static str),

    /// Compare-and-swap revision check failed because the registry was modified concurrently.
    #[error(
        "Registry revision changed (expected {expected}, actual {actual}); reload and try again."
    )]
    StaleRevision {
        /// Revision expected by the caller.
        expected: u64,
        /// Current revision observed on disk under the cross-process lock.
        actual: u64,
    },

    /// Registry collection count or serialized snapshot size exceeded its bounded capacity.
    #[error("{0}")]
    CapacityExceeded(&'static str),

    /// Registry directory, lock file, or snapshot file failed symlink, type, or permission checks.
    #[error("{0}")]
    SecurityViolation(&'static str),

    /// On-disk registry snapshot is malformed, exceeds 1 MiB, or violates integrity invariants.
    #[error("{0}")]
    CorruptStore(&'static str),

    /// Underlying filesystem or lock I/O operation failed.
    #[error("{0}")]
    Io(&'static str),
}

impl RegistryError {
    /// Returns a stable `snake_case` machine-readable error code for IPC and UI handling.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::InvalidReference(_) => "invalid_reference",
            Self::DuplicateAssignment(_) => "duplicate_assignment",
            Self::UnsupportedEffort(_) => "unsupported_effort",
            Self::ReferenceConflict(_) => "reference_conflict",
            Self::MissingAssignment(_) => "missing_assignment",
            Self::NotFound(_) => "not_found",
            Self::StaleRevision { .. } => "stale_revision",
            Self::CapacityExceeded(_) => "capacity_exceeded",
            Self::SecurityViolation(_) => "security_violation",
            Self::CorruptStore(_) => "corrupt_store",
            Self::Io(_) => "io_error",
        }
    }
}

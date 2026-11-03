//! Strict JSON response parser and identity verifier for the Coworker task `Reviewer` role.
//!
//! This module implements pure parsing, schema/identity verification, and deterministic verdict ID
//! derivation (`StructuredReviewVerdict` proposal construction only). It never issues acceptance
//! receipts, mutates workflow stores, advances task stages, or executes commands; the supervised
//! `Reviewer` execution runner is pending in a subsequent checkpoint.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::contracts::{ReviewVerdictOutcome, StructuredReviewVerdict};
use crate::{NativeError, NativeErrorCode};

/// Exact schema version required on every [`StructuredTaskReviewModelResponse`].
pub const TASK_REVIEW_RESPONSE_SCHEMA_VERSION: u32 = 1;

/// Maximum permitted raw byte length for a task `Reviewer` response payload (64 KiB).
pub const MAX_TASK_REVIEW_RESPONSE_BYTES: usize = 64 * 1024;

/// Exact 9-field task and candidate identity that a `Reviewer` model response must echo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedTaskReviewIdentity {
    /// Target task identifier.
    pub task_id: String,
    /// 1-based task attempt number that produced the candidate.
    pub attempt_no: u32,
    /// Target candidate identifier.
    pub candidate_id: String,
    /// 64-character lowercase hex SHA-256 digest of the inspected `ReviewSubject`.
    pub review_subject_digest: String,
    /// Exact 40-character lowercase hex base commit SHA of the reviewed diff.
    pub base_commit: String,
    /// Exact 40-character lowercase hex task head commit SHA of the reviewed candidate.
    pub task_head_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of the reviewed candidate.
    pub task_tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the scoped diff.
    pub diff_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the governing `ExecutionPolicy`.
    pub policy_digest: String,
}

/// Strict wire schema returned by the task `Reviewer` model before software verification.
///
/// Unknown fields (such as model-supplied `verdict_id` or `prose_may_advance_acceptance`) are
/// rejected at deserialization time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredTaskReviewModelResponse {
    /// Wire schema version (must equal [`TASK_REVIEW_RESPONSE_SCHEMA_VERSION`]).
    pub schema_version: u32,
    /// Target task identifier echoed by the reviewer.
    pub task_id: String,
    /// 1-based task attempt number echoed by the reviewer.
    pub attempt_no: u32,
    /// Target candidate identifier echoed by the reviewer.
    pub candidate_id: String,
    /// 64-character lowercase hex SHA-256 digest of the inspected `ReviewSubject`.
    pub review_subject_digest: String,
    /// Exact 40-character lowercase hex base commit SHA of the reviewed diff.
    pub base_commit: String,
    /// Exact 40-character lowercase hex task head commit SHA of the reviewed candidate.
    pub task_head_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of the reviewed candidate.
    pub task_tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the scoped diff.
    pub diff_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the governing `ExecutionPolicy`.
    pub policy_digest: String,
    /// Structured verdict outcome (`accept`, `correct_in_scope`, `scope_conflict`, `blocked`, or `unclear`).
    pub outcome: ReviewVerdictOutcome,
    /// Bounded structured findings reported by the reviewer.
    #[serde(default)]
    pub findings: Vec<String>,
    /// Required in-scope fixes (must be non-empty for `CorrectInScope` and empty for `Accept`).
    #[serde(default)]
    pub required_in_scope_fixes: Vec<String>,
    /// Conflicting paths or interface identifiers (must be non-empty for `ScopeConflict` and empty for `Accept`).
    #[serde(default)]
    pub conflicting_paths_or_interfaces: Vec<String>,
    /// Optional bounded reviewer prose summary (informational only; never advances acceptance).
    #[serde(default)]
    pub summary: Option<String>,
}

impl StructuredTaskReviewModelResponse {
    /// Parses a bounded raw JSON object and verifies its schema, identity, and verdict semantics.
    pub fn parse_and_verify(
        raw: &str,
        expected: &ExpectedTaskReviewIdentity,
    ) -> Result<StructuredReviewVerdict, NativeError> {
        parse_and_verify(raw, expected)
    }

    /// Verifies this parsed model response against `expected` and builds a [`StructuredReviewVerdict`].
    pub fn into_verified_verdict(
        self,
        expected: &ExpectedTaskReviewIdentity,
    ) -> Result<StructuredReviewVerdict, NativeError> {
        if self.schema_version != TASK_REVIEW_RESPONSE_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "unsupported task review schema_version {}; expected {}",
                    self.schema_version, TASK_REVIEW_RESPONSE_SCHEMA_VERSION
                ),
            ));
        }
        verify_expected_identity(&self, expected)?;

        let verdict_id = derive_verdict_id(&self);
        let verdict = StructuredReviewVerdict {
            verdict_id,
            task_id: self.task_id,
            attempt_no: self.attempt_no,
            candidate_id: self.candidate_id,
            review_subject_digest: self.review_subject_digest,
            base_commit: self.base_commit,
            task_head_commit: self.task_head_commit,
            task_tree_sha: self.task_tree_sha,
            diff_digest: self.diff_digest,
            policy_digest: self.policy_digest,
            outcome: self.outcome,
            findings: self.findings,
            required_in_scope_fixes: self.required_in_scope_fixes,
            conflicting_paths_or_interfaces: self.conflicting_paths_or_interfaces,
            prose_summary: self.summary,
            prose_may_advance_acceptance: false,
        };
        verdict.validate()?;
        Ok(verdict)
    }
}

/// Parses a bounded raw JSON `Reviewer` response, verifies its schema version and exact 9-field
/// identity against `expected`, derives a software-controlled `verdict_id`, forces
/// `prose_may_advance_acceptance = false`, and validates the resulting [`StructuredReviewVerdict`].
///
/// Even when the returned verdict has `ReviewVerdictOutcome::Accept`, it is only a validated typed
/// proposal; this parser never issues receipts, accepts a job, or executes commands.
pub fn parse_and_verify(
    raw: &str,
    expected: &ExpectedTaskReviewIdentity,
) -> Result<StructuredReviewVerdict, NativeError> {
    if raw.len() > MAX_TASK_REVIEW_RESPONSE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task review response length {} exceeds maximum {} bytes",
                raw.len(),
                MAX_TASK_REVIEW_RESPONSE_BYTES
            ),
        ));
    }

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response must not be empty",
        ));
    }
    if trimmed.starts_with("```") || trimmed.ends_with("```") {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response must be a raw JSON object without markdown fences",
        ));
    }
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response must be a single raw JSON object without surrounding prose",
        ));
    }

    let response: StructuredTaskReviewModelResponse =
        serde_json::from_str(trimmed).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("invalid structured task review JSON response: {err}"),
            )
        })?;

    response.into_verified_verdict(expected)
}

fn verify_expected_identity(
    response: &StructuredTaskReviewModelResponse,
    expected: &ExpectedTaskReviewIdentity,
) -> Result<(), NativeError> {
    if response.task_id != expected.task_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response task_id does not match expected identity",
        ));
    }
    if response.attempt_no != expected.attempt_no {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response attempt_no does not match expected identity",
        ));
    }
    if response.candidate_id != expected.candidate_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response candidate_id does not match expected identity",
        ));
    }
    if response.review_subject_digest != expected.review_subject_digest {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response review_subject_digest does not match expected identity",
        ));
    }
    if response.base_commit != expected.base_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response base_commit does not match expected identity",
        ));
    }
    if response.task_head_commit != expected.task_head_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response task_head_commit does not match expected identity",
        ));
    }
    if response.task_tree_sha != expected.task_tree_sha {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response task_tree_sha does not match expected identity",
        ));
    }
    if response.diff_digest != expected.diff_digest {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response diff_digest does not match expected identity",
        ));
    }
    if response.policy_digest != expected.policy_digest {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task review response policy_digest does not match expected identity",
        ));
    }
    Ok(())
}

fn derive_verdict_id(response: &StructuredTaskReviewModelResponse) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-task-review-verdict-v1\0");
    hasher.update(response.schema_version.to_be_bytes());
    update_len_prefixed(&mut hasher, &response.task_id);
    hasher.update(response.attempt_no.to_be_bytes());
    update_len_prefixed(&mut hasher, &response.candidate_id);
    update_len_prefixed(&mut hasher, &response.review_subject_digest);
    update_len_prefixed(&mut hasher, &response.base_commit);
    update_len_prefixed(&mut hasher, &response.task_head_commit);
    update_len_prefixed(&mut hasher, &response.task_tree_sha);
    update_len_prefixed(&mut hasher, &response.diff_digest);
    update_len_prefixed(&mut hasher, &response.policy_digest);
    update_len_prefixed(&mut hasher, response.outcome.as_str());
    update_list(&mut hasher, &response.findings);
    update_list(&mut hasher, &response.required_in_scope_fixes);
    update_list(&mut hasher, &response.conflicting_paths_or_interfaces);
    match &response.summary {
        Some(summary) => {
            hasher.update([1u8]);
            update_len_prefixed(&mut hasher, summary);
        }
        None => hasher.update([0u8]),
    }

    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("vrd_{hex}")
}

fn update_len_prefixed(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn update_list(hasher: &mut Sha256, items: &[String]) {
    hasher.update((items.len() as u64).to_be_bytes());
    for item in items {
        update_len_prefixed(hasher, item);
    }
}

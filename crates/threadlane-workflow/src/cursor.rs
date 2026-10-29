use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::WorkflowError;
use crate::types::MAX_CURSOR_LEN;

const CURSOR_PREFIX: &str = "tlwf1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorResource {
    Catalog,
    ListPlans,
    PlanTasks,
}

impl CursorResource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::ListPlans => "list_plans",
            Self::PlanTasks => "plan_tasks",
        }
    }

    pub fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "catalog" => Some(Self::Catalog),
            "list_plans" => Some(Self::ListPlans),
            "plan_tasks" => Some(Self::PlanTasks),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorSpec<'a> {
    pub resource: CursorResource,
    pub binding_id: &'a str,
    pub filter_tag: String,
    pub store_schema_version: u32,
    pub revision_watermark: u64,
    pub state_tag: String,
}

pub(crate) fn short_tag<T: Serialize + ?Sized>(
    domain: &str,
    value: &T,
) -> Result<String, WorkflowError> {
    let Ok(encoded) = serde_json::to_vec(&(domain, value)) else {
        return Err(WorkflowError::Serialization(
            "Failed to encode cursor digest payload.",
        ));
    };
    let digest = Sha256::digest(&encoded);
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(&mut out, "{byte:02x}");
    }
    Ok(out)
}

pub(crate) fn encode_cursor(
    spec: &CursorSpec<'_>,
    next_offset: usize,
    anchor_item_id: &str,
) -> Result<String, WorkflowError> {
    if next_offset == 0 {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor offset must be positive.",
        ));
    }
    let binding_tag = short_tag("binding", spec.binding_id)?;
    let anchor_tag = short_tag("anchor", anchor_item_id)?;
    let token = format!(
        "{CURSOR_PREFIX}:{}:{}:{}:{}:{}:{}:{}:{}",
        spec.resource.as_str(),
        binding_tag,
        spec.filter_tag,
        spec.store_schema_version,
        spec.revision_watermark,
        spec.state_tag,
        next_offset,
        anchor_tag
    );
    if token.len() > MAX_CURSOR_LEN {
        return Err(WorkflowError::InvalidCursor(
            "Generated pagination cursor exceeds maximum length.",
        ));
    }
    Ok(token)
}

pub(crate) fn decode_and_validate_cursor(
    raw: &str,
    spec: &CursorSpec<'_>,
    item_ids: &[&str],
) -> Result<usize, WorkflowError> {
    if raw.is_empty() {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor must not be empty.",
        ));
    }
    if raw.len() > MAX_CURSOR_LEN {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor exceeds maximum length.",
        ));
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor contains invalid whitespace or control characters.",
        ));
    }

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 9 || parts[0] != CURSOR_PREFIX {
        return Err(WorkflowError::InvalidCursor(
            "Malformed pagination cursor format.",
        ));
    }

    let Some(actual_resource) = CursorResource::from_str(parts[1]) else {
        return Err(WorkflowError::InvalidCursor(
            "Unrecognized pagination cursor resource kind.",
        ));
    };
    if actual_resource != spec.resource {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor resource kind does not match request.",
        ));
    }

    let cursor_binding_tag = parts[2];
    let cursor_filter_tag = parts[3];
    let cursor_schema_raw = parts[4];
    let cursor_revision_raw = parts[5];
    let cursor_state_tag = parts[6];
    let cursor_offset_raw = parts[7];
    let cursor_anchor_tag = parts[8];

    if !is_hex_16(cursor_binding_tag)
        || !is_hex_16(cursor_filter_tag)
        || !is_hex_16(cursor_state_tag)
        || !is_hex_16(cursor_anchor_tag)
    {
        return Err(WorkflowError::InvalidCursor(
            "Malformed pagination cursor integrity tag.",
        ));
    }

    let Ok(cursor_schema) = cursor_schema_raw.parse::<u32>() else {
        return Err(WorkflowError::InvalidCursor(
            "Malformed pagination cursor schema version.",
        ));
    };
    let Ok(cursor_revision) = cursor_revision_raw.parse::<u64>() else {
        return Err(WorkflowError::InvalidCursor(
            "Malformed pagination cursor revision watermark.",
        ));
    };
    let Ok(offset) = cursor_offset_raw.parse::<usize>() else {
        return Err(WorkflowError::InvalidCursor(
            "Malformed pagination cursor offset.",
        ));
    };
    if offset == 0 {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor offset must be positive.",
        ));
    }

    let expected_binding_tag = short_tag("binding", spec.binding_id)?;
    if cursor_binding_tag != expected_binding_tag {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor binding scope does not match bound chat.",
        ));
    }

    if cursor_filter_tag != spec.filter_tag {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor filter does not match request.",
        ));
    }

    if cursor_schema != spec.store_schema_version
        || cursor_revision != spec.revision_watermark
        || cursor_state_tag != spec.state_tag
    {
        return Err(WorkflowError::StaleRevision(
            "Pagination cursor is stale; reload from the first page.",
        ));
    }

    if offset >= item_ids.len() {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor position is out of bounds.",
        ));
    }

    let Some(anchor_id) = item_ids.get(offset - 1).copied() else {
        return Err(WorkflowError::InvalidCursor(
            "Pagination cursor anchor item is missing.",
        ));
    };
    let expected_anchor_tag = short_tag("anchor", anchor_id)?;
    if cursor_anchor_tag != expected_anchor_tag {
        return Err(WorkflowError::StaleRevision(
            "Pagination cursor anchor no longer matches store state.",
        ));
    }

    Ok(offset)
}

fn is_hex_16(s: &str) -> bool {
    s.len() == 16
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

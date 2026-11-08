/**
 * Pure, DOM-free diagnostic row projection for the Projects observation view.
 *
 * Maps raw observation warnings, structured warning contexts, source availability,
 * and pagination flags into user-facing diagnostic items with truthful source
 * provenance and without fictional conversation/job attribution.
 */
import type {
  ObservationSnapshot,
  ObservationSource,
  ObservationWarningContext,
} from '../observation/types.ts';
import {
  compactFeedText,
  type ActivityFeedScope,
} from './activity-feed-model.ts';

export type ProjectDiagnosticSeverity = 'warning' | 'info';

export interface ProjectDiagnosticRowView {
  key: string;
  severity: ProjectDiagnosticSeverity;
  title: string;
  sourceId: string | null;
  sourceLabel: string;
  area: string;
  conversationNote: string;
  impact: string;
  nextStep: string;
  technicalDetails: string;
  rawWarning: string;
  origin: string | null;
}

export interface ProjectDiagnosticsOptions {
  scope?: ActivityFeedScope | null;
}

export const HAS_MORE_DIAGNOSTIC_WARNING =
  'More request activity is available beyond this bounded observation page (has_more).';
export const JOBS_HAS_MORE_DIAGNOSTIC_WARNING =
  'More jobs exist than this bounded observation page shows (jobs_has_more).';

const SOURCE_NOT_RECORDED_LABEL = 'Source not recorded (source unknown)';
const GLOBAL_PAGE_SOURCE_LABEL = 'All observed sources (bounded page)';
const EXACT_CONVERSATION_UNKNOWN_NOTE = 'Exact conversation not identified';
const SCOPED_CONVERSATION_UNKNOWN_NOTE =
  'Exact conversation not identified — recorded for this source, not proven to belong to the selected job.';
const PAGE_WINDOW_SCOPE_NOTE = 'Applies to the current bounded observation page';

interface ClassifiedWarningTemplate {
  severity: ProjectDiagnosticSeverity;
  title: string;
  area: string;
  impact: string;
  nextStep: string;
  kind: 'source_history' | 'paging' | 'registry' | 'source_unavailable';
}

function trimOrNull(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeSourceId(value: string | null | undefined): string | null {
  const trimmed = trimOrNull(value);
  if (
    trimmed === null ||
    trimmed.length > 128 ||
    trimmed !== value ||
    !/^[A-Za-z0-9_.-]+$/.test(trimmed)
  ) {
    return null;
  }
  return trimmed;
}

function normalizeOrigin(value: string | null | undefined): string | null {
  const trimmed = trimOrNull(value);
  if (
    trimmed === 'registry' ||
    trimmed === 'jobs' ||
    trimmed === 'journal' ||
    trimmed === 'requests' ||
    trimmed === 'snapshot'
  ) {
    return trimmed;
  }
  return null;
}

interface NormalizedDiagnosticScope {
  source_id: string;
  job_id: string | null;
}

function normalizeDiagnosticScope(
  scope: ActivityFeedScope | null | undefined
): NormalizedDiagnosticScope | null {
  if (!scope || typeof scope !== 'object') {
    return null;
  }
  const sourceId = normalizeSourceId(scope.source_id);
  if (sourceId === null) {
    return null;
  }
  const jobId = normalizeSourceId(scope.job_id);
  return {
    source_id: sourceId,
    job_id: jobId,
  };
}

function splitProjectPathSegments(
  projectPath: string | null | undefined
): string[] {
  const trimmed = trimOrNull(projectPath);
  if (trimmed === null) {
    return [];
  }
  const normalized = trimmed.replace(/\\+/g, '/').replace(/\/+$/g, '');
  return normalized.split('/').filter((segment) => segment.length > 0);
}

function resolveDistinguishingProjectPath(
  source: ObservationSource,
  sourcesById: ReadonlyMap<string, ObservationSource>
): string | null {
  const segments = splitProjectPathSegments(source.project_path);
  if (segments.length === 0) {
    return null;
  }

  const normalizedOwnPath = segments.join('/');
  const collidingSegments: string[][] = [];
  for (const candidate of sourcesById.values()) {
    if (candidate.id === source.id) {
      continue;
    }
    const candidateSegments = splitProjectPathSegments(candidate.project_path);
    if (
      candidateSegments.length > 0 &&
      candidateSegments.join('/') !== normalizedOwnPath &&
      candidateSegments[candidateSegments.length - 1] ===
        segments[segments.length - 1]
    ) {
      collidingSegments.push(candidateSegments);
    }
  }

  if (collidingSegments.length === 0) {
    return segments[segments.length - 1]!;
  }

  for (let depth = 2; depth <= segments.length; depth += 1) {
    const suffix = segments.slice(segments.length - depth).join('/');
    const collides = collidingSegments.some((other) => {
      const otherSuffix = other
        .slice(Math.max(0, other.length - depth))
        .join('/');
      return otherSuffix === suffix;
    });
    if (!collides) {
      return suffix;
    }
  }

  return normalizedOwnPath;
}

function formatResolvedSourceLabel(
  sourceId: string,
  sourcesById: ReadonlyMap<string, ObservationSource>
): string {
  const source = sourcesById.get(sourceId);
  if (!source) {
    return 'Source details unavailable (project path unknown)';
  }
  const label = trimOrNull(source.label);
  const usableLabel = label !== null && label !== source.id ? label : null;
  const projectIdentity = resolveDistinguishingProjectPath(source, sourcesById);
  if (usableLabel !== null && projectIdentity !== null) {
    return `${usableLabel} (${projectIdentity})`;
  }
  if (usableLabel !== null) {
    return `${usableLabel} (project path unknown)`;
  }
  if (projectIdentity !== null) {
    return `Project ${projectIdentity}`;
  }
  return 'Source details unavailable (project path unknown)';
}

function formatTechnicalDetails(
  rawWarning: string,
  sourceId: string | null,
  origin: string | null
): string {
  const metaParts: string[] = [
    `source: ${sourceId ?? 'not recorded'}`,
  ];
  if (origin !== null) {
    metaParts.push(`origin: ${origin}`);
  }
  return `${rawWarning} (${metaParts.join(', ')})`;
}

function classifyWarningMessage(rawWarning: string): ClassifiedWarningTemplate {
  const normalized = rawWarning.trim();

  if (
    normalized ===
    'Skipped unreadable or invalid coworker workflow ledger in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Some work history could not be loaded',
      area: 'Workflow history',
      impact:
        'Some workflow history could not be read and was skipped; this is not proof of a worker failure.',
      nextStep:
        'Inspect history source compatibility and ledger file readability.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Skipped unreadable or invalid coworker controller runtime ledger in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Some execution history could not be loaded',
      area: 'Execution history',
      impact:
        'Runtime execution history is incomplete because the controller runtime ledger could not be read or validated.',
      nextStep:
        'Inspect execution history source readability and controller ledger compatibility.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Ignored unverified job, plan, task, or approval hint on journal record.'
  ) {
    return {
      severity: 'warning',
      title: 'Job association not verified',
      area: 'Activity correlation',
      impact:
        'A journal record carried an unverified job, plan, task, or approval hint; affected activity is shown without that association.',
      nextStep:
        'Information only — unverified hints are ignored so activity is never attached to the wrong job.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Detected conflicting scope fields across journal phases for a request.'
  ) {
    return {
      severity: 'warning',
      title: 'Inconsistent history association',
      area: 'Activity correlation',
      impact:
        'A request recorded conflicting scope fields across journal phases and cannot be safely attached to a conversation or job.',
      nextStep:
        'Inspect standalone request activity; conflicting conversation or job associations were withheld.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Rejected plan request link due to mismatched plan ID between record and finish result.'
  ) {
    return {
      severity: 'warning',
      title: 'Plan and activity link rejected',
      area: 'Workflow history',
      impact:
        'A plan/activity link was rejected because the record and finish result reported different plan IDs; history association is incomplete.',
      nextStep:
        'Review plan revisions in the project source; the request remains visible without the mismatched plan link.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Rejected unverified or mismatched parent request hint; kept request standalone.'
  ) {
    return {
      severity: 'warning',
      title: 'Parent request association rejected',
      area: 'Request hierarchy',
      impact:
        'A parent request association could not be verified or matched; the request is shown independently.',
      nextStep:
        'Information only — the request remains visible as a standalone entry.',
      kind: 'source_history',
    };
  }

  if (normalized === HAS_MORE_DIAGNOSTIC_WARNING) {
    return {
      severity: 'info',
      title: 'More request activity available',
      area: 'Activity paging',
      impact:
        'Information only — more request history can be loaded beyond this bounded observation page; this is not a fault.',
      nextStep:
        'Information only — use Load more history when you want to view older request activity.',
      kind: 'paging',
    };
  }

  if (
    normalized === JOBS_HAS_MORE_DIAGNOSTIC_WARNING ||
    /^Observation job list reached the maximum display cap\b/.test(normalized)
  ) {
    return {
      severity: 'info',
      title: 'More jobs exist beyond page limit',
      area: 'Job paging',
      impact:
        'Information only — older jobs beyond the bounded display limit are omitted from this page; this is not a fault.',
      nextStep:
        'Information only — newest jobs are shown first in this bounded view.',
      kind: 'paging',
    };
  }

  if (/^Activity paging reached the \d+-request window limit\b/.test(normalized)) {
    return {
      severity: 'info',
      title: 'Activity window limit reached',
      area: 'Activity paging',
      impact:
        'Information only — this refresh reached the maximum request window size; older activity was not loaded yet.',
      nextStep:
        'Information only — use Load more history to continue loading older requests if needed.',
      kind: 'paging',
    };
  }

  if (/^Activity paging stopped\b/.test(normalized)) {
    return {
      severity: 'warning',
      title: 'Activity paging stopped early',
      area: 'Activity paging',
      impact:
        'Pagination stopped before the requested activity window finished loading.',
      nextStep:
        'Refresh or use Load more history to retry loading older activity.',
      kind: 'paging',
    };
  }

  if (
    normalized ===
    'Skipped unreadable or invalid plan ledger in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Plan ledger skipped',
      area: 'Plan history',
      impact:
        'Plan history in this observation source could not be read or validated and was skipped.',
      nextStep: 'Inspect local plan ledger readability and compatibility.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Skipped unreadable or invalid approval ledger in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Approval ledger skipped',
      area: 'Approval history',
      impact:
        'Approval history in this observation source could not be read or validated and was skipped.',
      nextStep: 'Inspect local approval ledger readability and compatibility.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Skipped unreadable or invalid runtime state database in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Runtime state database skipped',
      area: 'Execution history',
      impact:
        'Runtime database records in this source could not be read in immutable read-only mode.',
      nextStep:
        'Wait for active database writes to checkpoint or inspect runtime state file permissions.',
      kind: 'source_history',
    };
  }

  if (
    normalized ===
    'Skipped unreadable or invalid session supervisor state in observation source.'
  ) {
    return {
      severity: 'warning',
      title: 'Session supervisor state skipped',
      area: 'Session history',
      impact:
        'Session supervisor state in this source could not be read or validated and was skipped.',
      nextStep: 'Inspect local session supervisor state readability.',
      kind: 'source_history',
    };
  }

  if (
    /^Source [A-Za-z0-9_.-]+ \((?:plans|runtime) journal\):/.test(normalized) ||
    /^Source [A-Za-z0-9_.-]+: failed to read (?:plans|runtime) observation journal in read-only mode\.$/.test(
      normalized
    )
  ) {
    return {
      severity: 'warning',
      title: 'Observation journal read warning',
      area: 'Activity history',
      impact:
        'Some observation journal records in this source could not be read cleanly and were skipped.',
      nextStep:
        'Inspect observation journal file readability and private directory permissions.',
      kind: 'source_history',
    };
  }

  if (
    /^Linked Threadlane source [A-Za-z0-9_.-]+ is unavailable\b/.test(normalized) ||
    /^Source .+ is unavailable\b/.test(normalized)
  ) {
    return {
      severity: 'warning',
      title: 'Observation source unavailable',
      area: 'Source availability',
      impact:
        'Jobs and activity from this observation source cannot be read right now.',
      nextStep:
        'Verify that the source context and local store directories exist with private owner-only permissions.',
      kind: 'source_unavailable',
    };
  }

  if (/^Additional observation warnings \(\d+ more\) were omitted\.$/.test(normalized)) {
    return {
      severity: 'info',
      title: 'Additional diagnostics omitted',
      area: 'Diagnostic summary',
      impact:
        'Information only — additional diagnostic messages beyond the display cap were omitted from this snapshot.',
      nextStep:
        'Resolve the listed source warnings above and refresh to inspect any remaining notices.',
      kind: 'paging',
    };
  }

  return {
    severity: 'warning',
    title: 'Observation diagnostic notice',
    area: 'Observation history',
    impact:
      'A diagnostic warning was reported while reading observation history; some history details may be incomplete.',
    nextStep:
      'Expand Technical details below to inspect the raw diagnostic message.',
    kind: 'source_history',
  };
}

interface ProvenanceGroup {
  sourceId: string | null;
  origin: string | null;
}

function collectWarningProvenances(
  warning: string,
  contextsByWarning: ReadonlyMap<string, ObservationWarningContext[]>
): ProvenanceGroup[] {
  const matching = contextsByWarning.get(warning);
  if (!matching || matching.length === 0) {
    return [{ sourceId: null, origin: null }];
  }

  const bySource = new Map<string, ProvenanceGroup>();
  for (const ctx of matching) {
    const sourceId = normalizeSourceId(ctx.source_id);
    const origin = normalizeOrigin(ctx.origin);
    const key = sourceId ?? '';
    const existing = bySource.get(key);
    if (!existing) {
      bySource.set(key, { sourceId, origin });
    } else if (existing.origin === null && origin !== null) {
      existing.origin = origin;
    }
  }
  return Array.from(bySource.values());
}

/**
 * Projects structured user-facing diagnostic rows from an `ObservationSnapshot`.
 *
 * Never guesses source provenance from a sole configured source or visible chat,
 * and never attributes source-level warnings to a specific conversation or job.
 */
export function projectDiagnostics(
  snapshot: ObservationSnapshot,
  options: ProjectDiagnosticsOptions = {}
): ProjectDiagnosticRowView[] {
  const scope = normalizeDiagnosticScope(options.scope);
  const sources = snapshot.sources ?? [];
  const sourcesById = new Map<string, ObservationSource>();
  for (const source of sources) {
    const id = normalizeSourceId(source.id);
    if (id !== null) {
      sourcesById.set(id, source);
    }
  }

  const contextsByWarning = new Map<string, ObservationWarningContext[]>();
  for (const rawContext of snapshot.warning_contexts ?? []) {
    if (!rawContext || typeof rawContext !== 'object') {
      continue;
    }
    const cleanedWarning = compactFeedText(rawContext.warning ?? '', 320);
    if (cleanedWarning.length === 0) {
      continue;
    }
    let list = contextsByWarning.get(cleanedWarning);
    if (!list) {
      list = [];
      contextsByWarning.set(cleanedWarning, list);
    }
    list.push({
      warning: cleanedWarning,
      source_id: normalizeSourceId(rawContext.source_id),
      origin: normalizeOrigin(rawContext.origin) ?? 'snapshot',
    });
  }

  const rows: ProjectDiagnosticRowView[] = [];
  const seenRowKeys = new Set<string>();
  const unavailableSourceIdsRendered = new Set<string>();

  const conversationNoteForMatchedSource =
    scope !== null && scope.job_id !== null
      ? SCOPED_CONVERSATION_UNKNOWN_NOTE
      : EXACT_CONVERSATION_UNKNOWN_NOTE;

  const pushRow = (row: ProjectDiagnosticRowView) => {
    if (seenRowKeys.has(row.key)) {
      return;
    }
    seenRowKeys.add(row.key);
    rows.push(row);
  };

  const uniqueWarnings: string[] = [];
  for (const rawWarning of snapshot.warnings ?? []) {
    const cleaned = compactFeedText(rawWarning, 320);
    if (cleaned.length > 0 && !uniqueWarnings.includes(cleaned)) {
      uniqueWarnings.push(cleaned);
    }
  }

  for (const warning of uniqueWarnings) {
    const template = classifyWarningMessage(warning);
    const provenances = collectWarningProvenances(warning, contextsByWarning);

    for (const provenance of provenances) {
      const { sourceId, origin } = provenance;

      if (
        scope !== null &&
        sourceId !== null &&
        sourceId !== scope.source_id
      ) {
        continue;
      }

      let sourceLabel: string;
      let conversationNote: string;

      if (sourceId !== null) {
        if (template.kind === 'source_unavailable') {
          unavailableSourceIdsRendered.add(sourceId);
        }
        sourceLabel = formatResolvedSourceLabel(sourceId, sourcesById);
        conversationNote =
          template.kind === 'paging'
            ? PAGE_WINDOW_SCOPE_NOTE
            : conversationNoteForMatchedSource;
      } else if (template.kind === 'paging') {
        sourceLabel = GLOBAL_PAGE_SOURCE_LABEL;
        conversationNote = PAGE_WINDOW_SCOPE_NOTE;
      } else {
        sourceLabel = SOURCE_NOT_RECORDED_LABEL;
        conversationNote = EXACT_CONVERSATION_UNKNOWN_NOTE;
      }

      const key = JSON.stringify([
        'diagnostic-warning',
        warning,
        sourceId ?? '',
      ]);

      pushRow({
        key,
        severity: template.severity,
        title: template.title,
        sourceId,
        sourceLabel,
        area: template.area,
        conversationNote,
        impact: template.impact,
        nextStep: template.nextStep,
        technicalDetails: formatTechnicalDetails(warning, sourceId, origin),
        rawWarning: warning,
        origin,
      });
    }
  }

  for (const source of sources) {
    if (source.availability === 'available') {
      continue;
    }
    const sourceId = normalizeSourceId(source.id);
    if (
      scope !== null &&
      sourceId !== null &&
      sourceId !== scope.source_id
    ) {
      continue;
    }
    if (sourceId !== null && unavailableSourceIdsRendered.has(sourceId)) {
      continue;
    }
    if (sourceId !== null) {
      unavailableSourceIdsRendered.add(sourceId);
    }

    const label = trimOrNull(source.label) ?? source.id;
    const message = trimOrNull(source.message);
    const rawWarning = compactFeedText(
      `Source ${label} is unavailable${message ? `: ${message}` : '.'}`,
      320
    );
    const sourceLabel =
      sourceId !== null
        ? formatResolvedSourceLabel(sourceId, sourcesById)
        : SOURCE_NOT_RECORDED_LABEL;
    const impact = message
      ? `Jobs and activity from this source cannot be read right now: ${compactFeedText(message, 220)}`
      : 'Jobs and activity from this source cannot be read right now.';

    pushRow({
      key: JSON.stringify(['diagnostic-unavailable-source', sourceId ?? label]),
      severity: 'warning',
      title: 'Observation source unavailable',
      sourceId,
      sourceLabel,
      area: 'Source availability',
      conversationNote: 'Affects all conversations in this source',
      impact,
      nextStep:
        'Verify that the local project store directories exist and have private owner-only permissions.',
      technicalDetails: formatTechnicalDetails(rawWarning, sourceId, 'registry'),
      rawWarning,
      origin: 'registry',
    });
  }

  if (
    snapshot.jobs_has_more === true &&
    !uniqueWarnings.includes(JOBS_HAS_MORE_DIAGNOSTIC_WARNING)
  ) {
    const template = classifyWarningMessage(JOBS_HAS_MORE_DIAGNOSTIC_WARNING);
    pushRow({
      key: JSON.stringify([
        'diagnostic-warning',
        JOBS_HAS_MORE_DIAGNOSTIC_WARNING,
        '',
      ]),
      severity: template.severity,
      title: template.title,
      sourceId: null,
      sourceLabel: GLOBAL_PAGE_SOURCE_LABEL,
      area: template.area,
      conversationNote: PAGE_WINDOW_SCOPE_NOTE,
      impact: template.impact,
      nextStep: template.nextStep,
      technicalDetails: formatTechnicalDetails(
        JOBS_HAS_MORE_DIAGNOSTIC_WARNING,
        null,
        'snapshot'
      ),
      rawWarning: JOBS_HAS_MORE_DIAGNOSTIC_WARNING,
      origin: 'snapshot',
    });
  }

  if (
    snapshot.has_more === true &&
    !uniqueWarnings.includes(HAS_MORE_DIAGNOSTIC_WARNING)
  ) {
    const template = classifyWarningMessage(HAS_MORE_DIAGNOSTIC_WARNING);
    pushRow({
      key: JSON.stringify([
        'diagnostic-warning',
        HAS_MORE_DIAGNOSTIC_WARNING,
        '',
      ]),
      severity: template.severity,
      title: template.title,
      sourceId: null,
      sourceLabel: GLOBAL_PAGE_SOURCE_LABEL,
      area: template.area,
      conversationNote: PAGE_WINDOW_SCOPE_NOTE,
      impact: template.impact,
      nextStep: template.nextStep,
      technicalDetails: formatTechnicalDetails(
        HAS_MORE_DIAGNOSTIC_WARNING,
        null,
        'snapshot'
      ),
      rawWarning: HAS_MORE_DIAGNOSTIC_WARNING,
      origin: 'snapshot',
    });
  }

  return rows;
}

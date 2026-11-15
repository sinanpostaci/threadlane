import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { applyPlanChangeLocally, applyRegistryChangeLocally } from './ipc.ts';
import {
  createEmptyRegistrySnapshot,
  resolve_choice,
  type PlanRevision,
  type RegistrySnapshot,
  type WorkflowProfile,
} from './types.ts';

function makeSnapshot(): RegistrySnapshot {
  return {
    ...createEmptyRegistrySnapshot(),
    revision: 1,
    tools: [
      {
        id: 'opencode',
        label: 'OpenCode Go',
        adapter: 'opencode_v2',
        executable_path: null,
        enabled: true,
      },
    ],
    models: [
      {
        id: 'deepseek-v4.1-flash',
        tool_id: 'opencode',
        label: 'DeepSeek v4.1 Flash',
        model_arg: 'opencode-go/deepseek-v4.1-flash',
        efforts: ['max'],
        allow_no_effort: false,
      },
    ],
    profiles: [],
    default_profile_id: null,
  };
}

function profileWithRole(role: 'architect' | 'advisor'): WorkflowProfile {
  return {
    id: `profile-${role}`,
    name: `Profile ${role}`,
    assignments: [
      {
        role,
        difficulty: null,
        choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
      },
    ],
  };
}

function planTask(role: 'architect' | 'advisor') {
  return {
    id: 'task-advisor',
    title: 'Advisor review',
    instruction: 'Return read-only advisory findings.',
    role,
    difficulty: 'hard' as const,
    depends_on: [],
    allowed_write_paths: ['src/lib.rs'],
  };
}

describe('advisor role compatibility at registry and plan boundaries', () => {
  it('stores new profile writes under advisor and resolves both aliases to the same exact choice', () => {
    const fromLegacy = applyRegistryChangeLocally(makeSnapshot(), 1, {
      op: 'upsert_profile',
      profile: profileWithRole('architect'),
    });
    assert.equal(fromLegacy.profiles[0]?.assignments[0]?.role, 'advisor');

    const withAdvisor = applyRegistryChangeLocally(fromLegacy, fromLegacy.revision, {
      op: 'upsert_profile',
      profile: profileWithRole('advisor'),
    });
    assert.equal(withAdvisor.profiles[1]?.assignments[0]?.role, 'advisor');

    const architectChoice = resolve_choice(
      withAdvisor,
      'profile-architect',
      'architect',
      null
    );
    const advisorChoice = resolve_choice(withAdvisor, 'profile-advisor', 'advisor', null);
    assert.deepEqual(architectChoice, {
      model_id: 'deepseek-v4.1-flash',
      effort: 'max',
    });
    assert.deepEqual(advisorChoice, architectChoice);
  });

  it('refuses architect and advisor duplicates for the same difficulty slot', () => {
    const snapshot = makeSnapshot();
    const source = structuredClone(snapshot);
    assert.throws(
      () =>
        applyRegistryChangeLocally(snapshot, snapshot.revision, {
          op: 'upsert_profile',
          profile: {
            id: 'profile-duplicate',
            name: 'Duplicate Advisor',
            assignments: [
              {
                role: 'architect',
                difficulty: 'hard',
                choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
              },
              {
                role: 'advisor',
                difficulty: 'hard',
                choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
              },
            ],
          },
        }),
      (error: unknown) =>
        (error as { code?: string }).code === 'duplicate_assignment'
    );
    assert.deepEqual(snapshot, source);
  });

  it('preserves raw legacy architect assignments on unrelated registry mutations', () => {
    const legacyProfile = profileWithRole('architect');
    const snapshot: RegistrySnapshot = {
      ...makeSnapshot(),
      profiles: [legacyProfile],
      default_profile_id: null,
    };
    const before = structuredClone(snapshot);

    const updated = applyRegistryChangeLocally(snapshot, snapshot.revision, {
      op: 'set_default_profile',
      profile_id: legacyProfile.id,
    });

    assert.equal(updated.default_profile_id, legacyProfile.id);
    assert.equal(updated.profiles[0]?.assignments[0]?.role, 'architect');
    assert.deepEqual(snapshot, before);
    assert.equal(legacyProfile.assignments[0]?.role, 'architect');
  });

  it('leaves stored profiles and revisions unchanged for a stale local write', () => {
    const snapshot = applyRegistryChangeLocally(makeSnapshot(), 1, {
      op: 'upsert_profile',
      profile: profileWithRole('advisor'),
    });
    const before = structuredClone(snapshot);
    assert.throws(
      () =>
        applyRegistryChangeLocally(snapshot, snapshot.revision - 1, {
          op: 'upsert_profile',
          profile: profileWithRole('architect'),
        }),
      (error: unknown) =>
        (error as { code?: string }).code === 'revision_conflict'
    );
    assert.deepEqual(snapshot, before);
  });

  it('accepts advisor and legacy architect plan input and stores advisor in new revisions', () => {
    const revisions: PlanRevision[] = [];
    const first = applyPlanChangeLocally(revisions, 0, {
      op: 'save_draft',
      plan_id: 'plan-advisor',
      binding_id: 'binding-1',
      profile_id: 'profile-advisor',
      profile_revision: 1,
      summary: 'Advisor plan',
      tasks: [planTask('advisor')],
    });
    assert.equal(first.updated.tasks[0]?.role, 'advisor');
    assert.equal(first.revisions[0]?.tasks[0]?.role, 'advisor');

    const second = applyPlanChangeLocally(first.revisions, 0, {
      op: 'save_draft',
      plan_id: 'plan-legacy',
      binding_id: 'binding-1',
      profile_id: 'profile-architect',
      profile_revision: 1,
      summary: 'Legacy input plan',
      tasks: [planTask('architect')],
    });
    assert.equal(second.updated.tasks[0]?.role, 'advisor');
    assert.equal(second.revisions[1]?.tasks[0]?.role, 'advisor');
  });

  it('fails closed on ambiguous duplicate slots instead of choosing the first assignment', () => {
    const snapshot: RegistrySnapshot = {
      ...makeSnapshot(),
      profiles: [
        {
          id: 'profile-ambiguous',
          name: 'Ambiguous Advisor',
          assignments: [
            {
              role: 'architect',
              difficulty: 'hard',
              choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
            },
            {
              role: 'advisor',
              difficulty: 'hard',
              choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
            },
          ],
        },
      ],
    };
    assert.throws(
      () => resolve_choice(snapshot, 'profile-ambiguous', 'architect', 'hard'),
      (error: unknown) =>
        (error as { code?: string }).code === 'duplicate_assignment'
    );
    assert.throws(
      () => resolve_choice(snapshot, 'profile-ambiguous', 'advisor', null),
      (error: unknown) =>
        (error as { code?: string }).code === 'duplicate_assignment'
    );
  });
});

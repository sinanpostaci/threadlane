import assert from 'node:assert/strict';
import Module from 'node:module';
import { describe, it } from 'node:test';

import {
  CANONICAL_WORKFLOW_ROLES,
  LEGACY_WORKFLOW_ROLES,
  WORKFLOW_ROLES,
  validatePlanTasksDag,
  workflowRoleLabel,
  type PlanRevision,
} from '../registry/ipc.ts';

if (typeof (Module as { registerHooks?: unknown }).registerHooks === 'function') {
  (
    Module as unknown as {
      registerHooks: (hooks: {
        load?: (
          url: string,
          context: unknown,
          nextLoad: (url: string, context: unknown) => unknown
        ) => unknown;
      }) => void;
    }
  ).registerHooks({
    load(url, context, nextLoad) {
      if (url.endsWith('.css')) {
        return { format: 'module', shortCircuit: true, source: 'export default {};' };
      }
      return nextLoad(url, context);
    },
  });
} else if (typeof Module.register === 'function') {
  const loaderSource = `
    export async function load(url, context, nextLoad) {
      if (url.endsWith('.css')) {
        return { format: 'module', shortCircuit: true, source: 'export default {};' };
      }
      return nextLoad(url, context);
    }
  `;
  Module.register(
    `data:text/javascript,${encodeURIComponent(loaderSource)}`,
    import.meta.url
  );
}

const { createInitialPlanDraft, createPlanDraftFromRevision, toPlanTaskDto } =
  await import('./plans.ts');

describe('plan role presentation', () => {
  it('presents canonical coder as Worker and advisor as Advisor without changing frozen values', () => {
    assert.deepEqual(
      WORKFLOW_ROLES.map((role) => workflowRoleLabel(role)),
      ['Follower', 'Orchestrator', 'Advisor', 'Worker', 'Reviewer', 'Closure']
    );
    assert.deepEqual(
      CANONICAL_WORKFLOW_ROLES.map((role) => workflowRoleLabel(role)),
      ['Follower', 'Orchestrator', 'Advisor', 'Worker', 'Reviewer', 'Closure']
    );
    assert.deepEqual(
      LEGACY_WORKFLOW_ROLES.map((role) => workflowRoleLabel(role)),
      ['Follower', 'Orchestrator', 'Advisor', 'Worker', 'Reviewer', 'Closure']
    );
    assert.equal(WORKFLOW_ROLES.includes('advisor' as never), true);
    assert.equal(WORKFLOW_ROLES.includes('architect' as never), false);
    assert.equal(CANONICAL_WORKFLOW_ROLES.includes('architect' as never), false);
    assert.equal(LEGACY_WORKFLOW_ROLES.includes('architect' as never), true);

    const task = createInitialPlanDraft().tasks[0];
    assert.ok(task, 'expected the default plan task');
    assert.equal(task.role, 'coder');
    assert.equal(workflowRoleLabel(task.role), 'Worker');
    assert.equal(toPlanTaskDto(task).role, 'coder');
  });

  it('accepts advisor and legacy architect plan input and emits advisor for new drafts', () => {
    const advisorTask = {
      ...createInitialPlanDraft().tasks[0]!,
      id: 'task-advisor',
      title: 'Advisor review',
      instruction: 'Read the plan and return advisory findings only.',
      role: 'advisor' as const,
      difficulty: 'hard' as const,
    };
    assert.equal(toPlanTaskDto(advisorTask).role, 'advisor');
    assert.equal(workflowRoleLabel('advisor'), 'Advisor');
    assert.equal(workflowRoleLabel('architect'), 'Advisor');
    assert.doesNotThrow(() => validatePlanTasksDag([toPlanTaskDto(advisorTask)]));

    const legacyTask = { ...advisorTask, role: 'architect' as const };
    assert.equal(toPlanTaskDto(legacyTask).role, 'advisor');
    assert.doesNotThrow(() => validatePlanTasksDag([toPlanTaskDto(legacyTask)]));
  });

  it('keeps a loaded historical plan revision raw until an explicit new save', () => {
    const revision: PlanRevision = {
      plan_id: 'plan-legacy',
      binding_id: 'binding-1',
      revision: 3,
      profile_id: 'profile-legacy',
      profile_revision: 2,
      summary: 'Historical advisor plan',
      tasks: [
        {
          id: 'task-legacy-advisor',
          title: 'Legacy advisor review',
          instruction: 'Read-only advisory findings.',
          role: 'architect',
          difficulty: 'hard',
          depends_on: [],
          allowed_write_paths: ['src/lib.rs'],
        },
      ],
      status: 'published',
      created_at_ms: 1,
    };

    const draft = createPlanDraftFromRevision(revision);
    assert.equal(draft.tasks[0]?.role, 'architect');
    assert.equal(revision.tasks[0]?.role, 'architect');
    assert.equal(workflowRoleLabel(revision.tasks[0]!.role), 'Advisor');
    assert.equal(toPlanTaskDto(draft.tasks[0]!).role, 'advisor');
  });
});

import assert from 'node:assert/strict';
import Module from 'node:module';
import { describe, it } from 'node:test';

import { WORKFLOW_ROLES, workflowRoleLabel } from '../registry/types.ts';

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

const { createInitialPlanDraft, toPlanTaskDto } = await import('./plans.ts');

describe('plan role presentation', () => {
  it('presents canonical coder as Worker without changing plan wire values', () => {
    assert.deepEqual(
      WORKFLOW_ROLES.map((role) => workflowRoleLabel(role)),
      ['Follower', 'Orchestrator', 'Architect', 'Worker', 'Reviewer', 'Closure']
    );

    const task = createInitialPlanDraft().tasks[0];
    assert.ok(task, 'expected the default plan task');
    assert.equal(task.role, 'coder');
    assert.equal(workflowRoleLabel(task.role), 'Worker');
    assert.equal(toPlanTaskDto(task).role, 'coder');
  });
});

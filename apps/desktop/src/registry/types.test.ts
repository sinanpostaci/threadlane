import assert from 'node:assert/strict';
import test from 'node:test';

import {
  EXECUTION_ROLES,
  EXECUTION_WORKFLOW_ROLES,
  WORKFLOW_ROLES,
  isExecutionRole,
  isExecutionWorkflowRole,
  isWorkflowRole,
} from './types.ts';

test('EXECUTION_ROLES defines the five configurable execution roles and excludes legacy follower', () => {
  assert.deepEqual(EXECUTION_ROLES, [
    'orchestrator',
    'architect',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(EXECUTION_ROLES.length, 5);
  assert.equal(EXECUTION_WORKFLOW_ROLES, EXECUTION_ROLES);
  assert.equal(EXECUTION_ROLES.includes('follower' as never), false);
});

test('WORKFLOW_ROLES retains legacy follower for historical snapshot and plan decoding without renaming to coworker', () => {
  assert.deepEqual(WORKFLOW_ROLES, [
    'follower',
    'orchestrator',
    'architect',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(WORKFLOW_ROLES.length, 6);
  assert.equal(isWorkflowRole('follower'), true);
  assert.equal(isWorkflowRole('coworker'), false);
  for (const role of EXECUTION_ROLES) {
    assert.equal(isWorkflowRole(role), true);
    assert.equal(isExecutionRole(role), true);
    assert.equal(isExecutionWorkflowRole(role), true);
  }
  assert.equal(isExecutionRole('follower'), false);
  assert.equal(isExecutionWorkflowRole('follower'), false);
  assert.equal(isExecutionRole('coworker'), false);
});

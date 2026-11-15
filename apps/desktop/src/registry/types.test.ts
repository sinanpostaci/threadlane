import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CANONICAL_EXECUTION_ROLES,
  CANONICAL_WORKFLOW_ROLES,
  EXECUTION_ROLES,
  EXECUTION_WORKFLOW_ROLES,
  LEGACY_EXECUTION_ROLES,
  LEGACY_WORKFLOW_ROLES,
  WORKFLOW_ROLES,
  canonicalExecutionWorkflowRole,
  canonicalWorkflowRole,
  findDuplicateWorkflowAssignment,
  isExecutionRole,
  isExecutionWorkflowRole,
  isWorkflowRole,
  workflowRoleConfigValue,
  workflowRoleLabel,
  workflowRoleSlotKey,
} from './types.ts';

test('CANONICAL_EXECUTION_ROLES defines the current five execution roles with Advisor exactly once', () => {
  assert.deepEqual(CANONICAL_EXECUTION_ROLES, [
    'orchestrator',
    'advisor',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(CANONICAL_EXECUTION_ROLES.length, 5);
  assert.equal(CANONICAL_EXECUTION_ROLES.includes('follower' as never), false);
  assert.equal(CANONICAL_EXECUTION_ROLES.includes('architect' as never), false);
  assert.equal(EXECUTION_WORKFLOW_ROLES, CANONICAL_EXECUTION_ROLES);
});

test('EXECUTION_ROLES is the current canonical execution list, while the legacy list is isolated', () => {
  assert.deepEqual(EXECUTION_ROLES, [
    'orchestrator',
    'advisor',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(EXECUTION_ROLES.length, 5);
  assert.equal(EXECUTION_ROLES.includes('architect' as never), false);
  assert.deepEqual(LEGACY_EXECUTION_ROLES, [
    'orchestrator',
    'architect',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(LEGACY_EXECUTION_ROLES.includes('advisor' as never), false);
});

test('WORKFLOW_ROLES uses advisor while the legacy workflow order stays explicitly readable', () => {
  assert.deepEqual(CANONICAL_WORKFLOW_ROLES, [
    'follower',
    'orchestrator',
    'advisor',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(CANONICAL_WORKFLOW_ROLES.length, 6);
  assert.equal(CANONICAL_WORKFLOW_ROLES.includes('architect' as never), false);
  assert.deepEqual(WORKFLOW_ROLES, CANONICAL_WORKFLOW_ROLES);
  assert.equal(WORKFLOW_ROLES.includes('architect' as never), false);
  assert.deepEqual(LEGACY_WORKFLOW_ROLES, [
    'follower',
    'orchestrator',
    'architect',
    'coder',
    'reviewer',
    'closure',
  ]);
  assert.equal(LEGACY_WORKFLOW_ROLES.includes('advisor' as never), false);
  assert.equal(isWorkflowRole('follower'), true);
  assert.equal(isWorkflowRole('coworker'), false);
  for (const role of CANONICAL_EXECUTION_ROLES) {
    assert.equal(isWorkflowRole(role), true);
    assert.equal(isExecutionRole(role), true);
    assert.equal(isExecutionWorkflowRole(role), true);
  }
  assert.equal(isExecutionRole('follower'), false);
  assert.equal(isExecutionWorkflowRole('follower'), false);
  assert.equal(isExecutionRole('coworker'), false);
});

test('legacy architect is a truthful input spelling and both aliases normalize to Advisor', () => {
  assert.equal(isWorkflowRole('advisor'), true);
  assert.equal(isWorkflowRole('architect'), true);
  assert.equal(isExecutionRole('advisor'), true);
  assert.equal(isExecutionRole('architect'), true);
  assert.equal(isExecutionRole('unknown-role'), false);

  assert.equal(canonicalWorkflowRole('advisor'), 'advisor');
  assert.equal(canonicalWorkflowRole('architect'), 'advisor');
  assert.equal(canonicalExecutionWorkflowRole('advisor'), 'advisor');
  assert.equal(canonicalExecutionWorkflowRole('architect'), 'advisor');
  assert.equal(canonicalExecutionWorkflowRole('coder'), 'coder');
  assert.equal(canonicalExecutionWorkflowRole('follower'), null);
  assert.equal(workflowRoleConfigValue('architect'), 'advisor');
  assert.equal(workflowRoleConfigValue('advisor'), 'advisor');
  assert.equal(workflowRoleConfigValue('coder'), 'coder');

  assert.equal(workflowRoleLabel('architect'), 'Advisor');
  assert.equal(workflowRoleLabel('advisor'), 'Advisor');
  assert.equal(workflowRoleLabel('coder'), 'Worker');
  assert.equal(workflowRoleSlotKey('architect', 'hard'), 'advisor:hard');
  assert.equal(workflowRoleSlotKey('advisor', 'hard'), 'advisor:hard');
  assert.equal(workflowRoleSlotKey('coder', 'hard'), 'coder:hard');
});

test('duplicate role/difficulty slots are detected across both configuration aliases', () => {
  assert.deepEqual(
    findDuplicateWorkflowAssignment([
      { role: 'architect', difficulty: 'hard' },
      { role: 'advisor', difficulty: 'hard' },
    ]),
    { role: 'advisor', difficulty: 'hard' }
  );
  assert.equal(
    findDuplicateWorkflowAssignment([
      { role: 'architect', difficulty: 'hard' },
      { role: 'advisor', difficulty: null },
    ]),
    null
  );
});

import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import { PaymentWorkflows, type PaymentIntent } from '../src/payments.js';
import { createSandbox } from '../support/sandbox.js';

let db: Awaited<ReturnType<typeof createSandbox>>;
let workflow: PaymentWorkflows;
const intent: PaymentIntent = {
  key: 'event-001', resourceId: 'resource-001', generation: 1,
  amountMinor: 2400, currency: 'USD',
};
before(async () => {
  db = await createSandbox();
  workflow = new PaymentWorkflows(db.pool);
});
after(async () => { if (db) await db.close(); });
beforeEach(async () => {
  await db.pool.query('TRUNCATE resources, payments');
  await workflow.createResource(intent.resourceId);
});

test('duplicate start, completion and cancellation return their persisted result', async () => {
  const started = await workflow.start(intent);
  assert.equal(started.state, 'pending');
  assert.deepEqual(await workflow.start(intent), started);
  const completed = await workflow.complete(intent.key);
  assert.equal(completed.state, 'completed');
  assert.ok(completed.completedAt);
  assert.deepEqual(await workflow.complete(intent.key), completed);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, intent.key);
  const canceled = await workflow.cancel(intent.key);
  assert.equal(canceled.state, 'canceled');
  assert.ok(canceled.canceledAt);
  assert.deepEqual(await workflow.cancel(intent.key), canceled);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
});

async function waitForBlockedConnections(count: number) {
  const deadline = Date.now() + 4000;
  let waiting: number[] = [];
  while (Date.now() < deadline) {
    waiting = await db.waitingConnections();
    if (waiting.length >= count) break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(new Set(waiting).size >= count, 'independent connections must contend on real row locks');
}

async function contend<T>(operations: Array<() => Promise<T>>, resourceIds = [intent.resourceId]) {
  const blocker = await db.pool.connect();
  await blocker.query('BEGIN');
  await blocker.query('SELECT id FROM resources WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE', [resourceIds]);
  const results = Promise.allSettled(operations.map((operation) => operation()));
  try {
    await waitForBlockedConnections(operations.length);
  } finally {
    await blocker.query('ROLLBACK');
    blocker.release();
  }
  return results;
}

test('an idempotency key rejects changes to any part of its intent', async () => {
  await workflow.start(intent);
  await workflow.createResource('resource-002');
  for (const change of [
    { amountMinor: 2401 }, { currency: 'EUR' }, { generation: 2 }, { resourceId: 'resource-002' },
  ]) {
    await assert.rejects(workflow.start({ ...intent, ...change }), { code: 'INTENT_MISMATCH' });
  }
  assert.equal((await db.pool.query('SELECT count(*)::int AS total FROM payments')).rows[0].total, 1);
});

test('concurrent starts with the same key create exactly one payment record', async () => {
  const results = await contend([() => workflow.start(intent), () => workflow.start(intent)]);
  assert.equal(results[0]?.status, 'fulfilled');
  assert.deepEqual(results[0], results[1]);
  assert.equal((await db.pool.query('SELECT count(*)::int AS total FROM payments')).rows[0].total, 1);
});

test('concurrent reuse of a key for different resources rejects the losing intent', async () => {
  await workflow.createResource('resource-002');
  const results = await contend([
    () => workflow.start(intent),
    () => workflow.start({ ...intent, resourceId: 'resource-002' }),
  ], [intent.resourceId, 'resource-002']);
  assert.equal(results.filter(({ status }) => status === 'fulfilled').length, 1);
  const rejected = results.find((result) => result.status === 'rejected');
  assert.equal(rejected?.reason.code, 'INTENT_MISMATCH');
});

test('concurrent completions share the persisted completion timestamp and entitlement', async () => {
  await workflow.start(intent);
  const results = await contend([() => workflow.complete(intent.key), () => workflow.complete(intent.key)]);
  assert.equal(results[0]?.status, 'fulfilled');
  assert.deepEqual(results[0], results[1]);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, intent.key);
});

test('concurrent cancellations share the persisted cancellation timestamp', async () => {
  await workflow.start(intent);
  await workflow.complete(intent.key);
  const results = await contend([() => workflow.cancel(intent.key), () => workflow.cancel(intent.key)]);
  assert.equal(results[0]?.status, 'fulfilled');
  assert.deepEqual(results[0], results[1]);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
});

test('completion racing cancellation cannot leave a canceled entitlement active', async () => {
  await workflow.start(intent);
  const results = await contend([() => workflow.complete(intent.key), () => workflow.cancel(intent.key)]);
  assert.equal(results[1]?.status, 'fulfilled');
  if (results[0]?.status === 'rejected') assert.equal(results[0].reason.code, 'PAYMENT_CANCELED');
  assert.equal((await workflow.getPayment(intent.key)).state, 'canceled');
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
});

test('a canceled pending payment stays terminal when completion arrives late', async () => {
  await workflow.start(intent);
  const canceled = await workflow.cancel(intent.key);
  assert.equal(canceled.completedAt, null);
  await assert.rejects(workflow.complete(intent.key), { code: 'PAYMENT_CANCELED' });
  assert.deepEqual(await workflow.cancel(intent.key), canceled);
});

test('an old pending payment cannot complete in a new generation', async () => {
  await workflow.start(intent);
  await workflow.advanceGeneration(intent.resourceId, 1);
  await assert.rejects(workflow.complete(intent.key), { code: 'STALE_GENERATION' });
  assert.equal((await workflow.getPayment(intent.key)).state, 'pending');
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
});

test('historical completion retries do not regrant an entitlement after renewal', async () => {
  await workflow.start(intent);
  const completed = await workflow.complete(intent.key);
  await workflow.advanceGeneration(intent.resourceId, 1);
  assert.deepEqual(await workflow.complete(intent.key), completed);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
});

test('canceling an older payment preserves the newer generation entitlement', async () => {
  await workflow.start(intent);
  await workflow.complete(intent.key);
  await workflow.advanceGeneration(intent.resourceId, 1);
  const next = { ...intent, key: 'event-002', generation: 2 };
  await workflow.start(next);
  await workflow.complete(next.key);
  await workflow.cancel(intent.key);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, next.key);
  assert.equal((await workflow.getPayment(next.key)).state, 'completed');
});

test('different payments cannot both acquire the same generation entitlement', async () => {
  await workflow.start(intent);
  await workflow.start({ ...intent, key: 'event-002' });
  const results = await contend([
    () => workflow.complete(intent.key), () => workflow.complete('event-002'),
  ]);
  assert.equal(results.filter(({ status }) => status === 'fulfilled').length, 1);
  assert.equal(results.find((result) => result.status === 'rejected')?.reason.code, 'ENTITLEMENT_CONFLICT');
  const completed = await db.pool.query("SELECT idempotency_key FROM payments WHERE state = 'completed'");
  assert.equal(completed.rowCount, 1);
  assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, completed.rows[0].idempotency_key);
});

test('generation advance racing completion never carries an entitlement into the new cycle', async () => {
  await workflow.start(intent);
  const results = await contend<unknown>([
    () => workflow.complete(intent.key), () => workflow.advanceGeneration(intent.resourceId, 1),
  ]);
  assert.equal(results[1]?.status, 'fulfilled');
  if (results[0]?.status === 'rejected') assert.equal(results[0].reason.code, 'STALE_GENERATION');
  assert.deepEqual(await workflow.getResource(intent.resourceId), {
    id: intent.resourceId, generation: 2, entitlementKey: null,
  });
});

test('a database failure after entitlement update rolls both changes back', async () => {
  await workflow.start(intent);
  await db.pool.query("ALTER TABLE payments ADD CONSTRAINT reject_completion CHECK (state <> 'completed')");
  try {
    await assert.rejects(workflow.complete(intent.key), { code: '23514' });
    assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, null);
    assert.equal((await workflow.getPayment(intent.key)).state, 'pending');
  } finally {
    await db.pool.query('ALTER TABLE payments DROP CONSTRAINT reject_completion');
  }
});

test('a database failure during cancellation preserves the completed entitlement', async () => {
  await workflow.start(intent);
  await workflow.complete(intent.key);
  await db.pool.query("ALTER TABLE payments ADD CONSTRAINT reject_cancellation CHECK (state <> 'canceled')");
  try {
    await assert.rejects(workflow.cancel(intent.key), { code: '23514' });
    assert.equal((await workflow.getResource(intent.resourceId)).entitlementKey, intent.key);
    assert.equal((await workflow.getPayment(intent.key)).state, 'completed');
  } finally {
    await db.pool.query('ALTER TABLE payments DROP CONSTRAINT reject_cancellation');
  }
});

test('invalid amounts and stale generation advances leave state unchanged', async () => {
  for (const amountMinor of [0, -1, 1.5, Number.MAX_SAFE_INTEGER]) {
    await assert.rejects(workflow.start({ ...intent, amountMinor }), { code: 'INVALID_INTENT' });
  }
  await assert.rejects(workflow.advanceGeneration(intent.resourceId, 2), { code: 'STALE_GENERATION' });
  assert.equal((await workflow.getResource(intent.resourceId)).generation, 1);
  assert.equal((await db.pool.query('SELECT count(*)::int AS total FROM payments')).rows[0].total, 0);
});

test('caller mutation during a lock wait cannot change the captured payment intent', async () => {
  await workflow.createResource('resource-002');
  const mutable = { ...intent };
  const blocker = await db.pool.connect();
  await blocker.query('BEGIN');
  await blocker.query('SELECT id FROM resources WHERE id = $1 FOR UPDATE', [intent.resourceId]);
  const pending = workflow.start(mutable);
  try {
    await waitForBlockedConnections(1);
    Object.assign(mutable, { key: 'mutated-event', resourceId: 'resource-002', amountMinor: 9900 });
  } finally {
    await blocker.query('ROLLBACK');
    blocker.release();
  }
  const result = await pending;
  assert.equal(result.key, intent.key);
  assert.equal(result.resourceId, intent.resourceId);
  assert.equal(result.amountMinor, intent.amountMinor);
  assert.deepEqual(await workflow.getPayment(intent.key), result);
});

test('keys and currency codes reject trailing line separators exactly', async () => {
  for (const separator of ['\n', '\r', '\u2028', '\u2029']) {
    await assert.rejects(workflow.getPayment(`event${separator}`), { code: 'INVALID_KEY' });
    await assert.rejects(workflow.start({ ...intent, key: `event${separator}` }), { code: 'INVALID_INTENT' });
    await assert.rejects(workflow.start({ ...intent, currency: `USD${separator}` }), { code: 'INVALID_INTENT' });
  }
});

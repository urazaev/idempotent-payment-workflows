import assert from 'node:assert/strict';
import { PaymentWorkflows } from '../src/payments.js';
import { createSandbox } from '../support/sandbox.js';

const database = await createSandbox();
try {
  const workflow = new PaymentWorkflows(database.pool);
  await workflow.createResource('resource-demo');
  const firstIntent = {
    key: 'payment-one',
    resourceId: 'resource-demo',
    generation: 1,
    amountMinor: 2400,
    currency: 'USD',
  };
  const [first, duplicate] = await Promise.all([
    workflow.start(firstIntent),
    workflow.start(firstIntent),
  ]);
  assert.deepEqual(first, duplicate);
  const completed = await workflow.complete(first.key);
  assert.deepEqual(await workflow.complete(first.key), completed);

  await workflow.advanceGeneration('resource-demo', 1);
  await workflow.start({ ...firstIntent, key: 'payment-two', generation: 2 });
  await workflow.complete('payment-two');
  await workflow.cancel('payment-one');
  const resource = await workflow.getResource('resource-demo');
  assert.equal(resource.entitlementKey, 'payment-two');

  console.log(
    JSON.stringify(
      {
        mode: 'synthetic events; PostgreSQL state only',
        duplicateStartWasStable: true,
        oldPayment: await workflow.getPayment('payment-one'),
        currentPayment: await workflow.getPayment('payment-two'),
        resource,
      },
      null,
      2,
    ),
  );
} finally {
  await database.close();
}
console.log('Old cancellation preserved the new entitlement. Temporary schema removed.');

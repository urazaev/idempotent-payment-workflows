# Idempotent payment workflows

[![Checks](https://github.com/urazaev/idempotent-payment-workflows/actions/workflows/check.yml/badge.svg?branch=main)](https://github.com/urazaev/idempotent-payment-workflows/actions/workflows/check.yml)

A small TypeScript and PostgreSQL example of handling duplicate and competing payment
events without splitting payment state from resource access.

The interesting part is the transaction boundary: callbacks lock the resource first, then
the payment; a lifecycle generation prevents old events from granting current access; and
cancellation can revoke only the entitlement that payment owns. The tests use real,
competing PostgreSQL connections.

**Synthetic events only.** This repository updates database state. It does not contact a
bank, process money, issue a refund, or call an external payment API.

## Run it

Requires **Node.js 22**, npm, and Docker Compose. Tested with PostgreSQL 15.

```sh
npm ci
docker compose up -d --wait
export TEST_DATABASE_URL='postgresql://demo:demo@127.0.0.1:55432/payment_demo'

npm run format:check
npm run check
npm test
npm run demo
```

The Compose credentials are disposable demo values. PostgreSQL listens only on loopback
port `55432`. Use this dedicated database, not an application database. If that port is
occupied, change the Compose mapping and the URL together.

Both the tests and demo create their own random schema and remove it on normal completion.
They do not load `.env` files or assume a default database. The runner accepts only a
loopback URL without query parameters or a fragment, so connection-string options cannot
override schema isolation. A killed process may leave its generated `payment_example_*`
schema behind.

Stop the example's database when finished:

```sh
docker compose down --volumes
```

## Start reading here

| File                                               | What to look for                                                                            |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| [`src/payments.ts`](src/payments.ts)               | `start`, `complete`, `cancel`, and the shared resource-before-payment lock order.           |
| [`src/transaction.ts`](src/transaction.ts)         | One connection per transaction, awaited commit, rollback, and an uncertain-commit boundary. |
| [`schema.sql`](schema.sql)                         | Unique idempotency keys, foreign keys, and valid state/timestamp combinations.              |
| [`test/workflows.test.ts`](test/workflows.test.ts) | Controlled lock contention, lifecycle races, and failures after the first write.            |
| [`examples/scenario.ts`](examples/scenario.ts)     | Duplicate requests followed by a new generation and an old cancellation.                    |

`support/sandbox.ts` is local test/demo infrastructure, separate from the workflow core.
The core receives a `pg.Pool` and does not read environment variables.

## The model

A **resource** has a generation and, optionally, the key of the payment granting its
current entitlement. A **payment** binds one globally unique idempotency key to an
immutable intent: resource ID, generation, amount in minor units, and currency.

```mermaid
stateDiagram-v2
    [*] --> pending: start
    pending --> completed: complete
    pending --> canceled: cancel
    completed --> canceled: cancel
    completed --> completed: duplicate complete
    canceled --> canceled: duplicate cancel
```

`canceled` is terminal. A late completion after cancellation is rejected. Payment history
and current entitlement are distinct: advancing a resource generation clears its
entitlement without rewriting older payment records.

```ts
const workflow = new PaymentWorkflows(pool);
await workflow.createResource('resource-001');
await workflow.start({
  key: 'event-001',
  resourceId: 'resource-001',
  generation: 1,
  amountMinor: 2400,
  currency: 'USD',
});
await workflow.complete('event-001');
await workflow.cancel('event-001');
```

## Guarantees inside the database

| Event                                          | Result                                                                                  |
| ---------------------------------------------- | --------------------------------------------------------------------------------------- |
| Same key, same intent                          | Returns the existing payment record.                                                    |
| Same key, changed intent                       | Rejects with `INTENT_MISMATCH`, including a race across two resources.                  |
| First valid completion                         | Updates payment and entitlement in one transaction.                                     |
| Duplicate completion                           | Returns the saved completion timestamp and state without granting access again.         |
| Cancellation                                   | Clears entitlement only if that payment still owns it; records cancellation atomically. |
| Duplicate cancellation                         | Returns the saved cancellation result.                                                  |
| Pending payment from an old generation         | Cannot complete against the new generation.                                             |
| Competing payments for one current entitlement | Only one can acquire it; the other stays pending.                                       |
| Failed second database write                   | Rolls back the first write too.                                                         |

Duplicate results are stable while no later valid state transition has occurred. For
example, after a completed payment is canceled, another completion is rejected rather than
returning a stale success or reviving access. `start` returns the payment's current
persisted state on retry.

Every mutation uses `READ COMMITTED` and one checked-out database connection. Resource-row
locks serialize operations affecting the same resource. Payment rows are locked afterward
and re-read, so decisions use the state visible after waiting. A unique index resolves
simultaneous attempts to reuse a key across different resources; the losing request
compares the winning intent before returning.

`advanceGeneration(id, expectedGeneration)` uses the same resource lock and rejects a
stale expected value. Repeating a historical completion after advancement returns history
only. Canceling an older payment cannot clear a newer payment's entitlement.

The transaction helper waits for PostgreSQL's commit acknowledgement. It does **not**
automatically retry database or connection errors: losing that acknowledgement can leave
the outcome uncertain. Reconcile through the same idempotency key before retrying. No
external side effect belongs inside the transaction callback.

## What the tests prove

The integration suite covers duplicate start/complete/cancel events, intent mismatches,
lifecycle boundaries, old-payment reversal, and concurrent entitlement acquisition. Race
tests hold a row lock, start operations on separate connections, confirm that PostgreSQL
reports blocked backends, then release the lock. This exercises actual lock contention
rather than an in-memory imitation.

Rollback tests add a temporary PostgreSQL constraint that rejects the payment update after
the resource update has run. They then assert that both records retain their prior state.
A separate regression mutates a caller's input during a lock wait and checks that the
original captured intent is persisted.

The demo prints a canceled first payment, a completed second payment, and a resource whose
current entitlement still belongs to the second payment. All data is generated locally.

## Scope and tradeoffs

- This is an inspectable transaction example, not a payment gateway, accounting ledger, or
  deployment template. There is no HTTP server, authentication, signature verification,
  provider reconciliation, or refund execution.
- Amount and currency are supplied by trusted application code in this example. A real
  boundary must derive and verify the payable intent; these methods must not accept
  unchecked browser claims as authority.
- The idempotency namespace is global and records are retained indefinitely. Tenant
  scoping, retention, key expiry, and provider-event mapping are outside this model.
- There is one current entitlement per resource. Row locking deliberately serializes work
  on a hot resource; this repository makes no throughput claim.
- The guarantee covers these cooperating database workflows. Direct writes that bypass
  them can violate business invariants even when basic schema constraints still pass.
- Exactly-once external delivery is not claimed. Receipts, email, webhooks, and other
  effects need a separately designed durable delivery mechanism.
- The local runner uses five-second lock and ten-second statement timeouts. Service-level
  deadlines, retries, observability, and recovery after an uncertain commit remain
  application decisions.

The transaction and parameterization approach follows the official
[node-postgres transaction guide](https://node-postgres.com/features/transactions),
[parameterized-query guide](https://node-postgres.com/features/queries), and
[PostgreSQL row-locking documentation](https://www.postgresql.org/docs/15/explicit-locking.html#LOCKING-ROWS).

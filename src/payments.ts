import type { Pool, PoolClient } from 'pg';
import { inTransaction } from './transaction.js';

export interface PaymentIntent {
  key: string;
  resourceId: string;
  generation: number;
  amountMinor: number;
  currency: string;
}
export interface Payment extends PaymentIntent {
  state: 'pending' | 'completed' | 'canceled';
  completedAt: string | null;
  canceledAt: string | null;
}
export interface Resource {
  id: string;
  generation: number;
  entitlementKey: string | null;
}

type ErrorCode = 'INVALID_INTENT' | 'INVALID_KEY' | 'RESOURCE_NOT_FOUND' |
  'PAYMENT_NOT_FOUND' | 'INTENT_MISMATCH' | 'STALE_GENERATION' |
  'PAYMENT_CANCELED' | 'ENTITLEMENT_CONFLICT';

export class WorkflowError extends Error {
  constructor(readonly code: ErrorCode) {
    super(code);
    this.name = 'WorkflowError';
  }
}

type Database = Pick<PoolClient, 'query'>;
interface PaymentRow {
  idempotency_key: string;
  resource_id: string;
  generation: number;
  amount_minor: number;
  currency: string;
  state: Payment['state'];
  completed_at: Date | null;
  canceled_at: Date | null;
}
interface ResourceRow {
  id: string;
  generation: number;
  entitlement_key: string | null;
}

const validKey = (value: unknown): value is string =>
  typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,79}$/.test(value);
const positiveInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value > 0 && value <= 2_147_483_647;

function requireKey(value: unknown): asserts value is string {
  if (!validKey(value)) throw new WorkflowError('INVALID_KEY');
}
function validateAndCopyIntent(input: PaymentIntent): PaymentIntent {
  // Capture scalars before any await; callers retain ownership of their object.
  const intent = {
    key: input.key,
    resourceId: input.resourceId,
    generation: input.generation,
    amountMinor: input.amountMinor,
    currency: input.currency,
  };
  if (!validKey(intent.key) || !validKey(intent.resourceId) ||
      !positiveInteger(intent.generation) || !positiveInteger(intent.amountMinor) ||
      typeof intent.currency !== 'string' || !/^[A-Z]{3}$/.test(intent.currency)) {
    throw new WorkflowError('INVALID_INTENT');
  }
  return intent;
}
function assertSameIntent(payment: Payment, intent: PaymentIntent) {
  if (payment.resourceId !== intent.resourceId || payment.generation !== intent.generation ||
      payment.amountMinor !== intent.amountMinor || payment.currency !== intent.currency) {
    throw new WorkflowError('INTENT_MISMATCH');
  }
}
const paymentFromRow = (row: PaymentRow): Payment => ({
  key: row.idempotency_key, resourceId: row.resource_id, generation: row.generation,
  amountMinor: row.amount_minor, currency: row.currency, state: row.state,
  completedAt: row.completed_at?.toISOString() ?? null,
  canceledAt: row.canceled_at?.toISOString() ?? null,
});
const resourceFromRow = (row: ResourceRow): Resource => ({
  id: row.id, generation: row.generation, entitlementKey: row.entitlement_key,
});

async function readResource(db: Database, id: string, lock = false): Promise<Resource> {
  const result = await db.query<ResourceRow>(
    `SELECT * FROM resources WHERE id = $1${lock ? ' FOR UPDATE' : ''}`, [id],
  );
  if (!result.rows[0]) throw new WorkflowError('RESOURCE_NOT_FOUND');
  return resourceFromRow(result.rows[0]);
}
async function findPayment(db: Database, key: string, lock = false): Promise<Payment | undefined> {
  const result = await db.query<PaymentRow>(
    `SELECT * FROM payments WHERE idempotency_key = $1${lock ? ' FOR UPDATE' : ''}`, [key],
  );
  return result.rows[0] ? paymentFromRow(result.rows[0]) : undefined;
}
async function readPayment(db: Database, key: string, lock = false): Promise<Payment> {
  const payment = await findPayment(db, key, lock);
  if (!payment) throw new WorkflowError('PAYMENT_NOT_FOUND');
  return payment;
}

export class PaymentWorkflows {
  constructor(private readonly pool: Pool) {}

  async createResource(id: string): Promise<Resource> {
    requireKey(id);
    const result = await this.pool.query<ResourceRow>(
      'INSERT INTO resources (id) VALUES ($1) RETURNING *', [id],
    );
    return resourceFromRow(result.rows[0]!);
  }

  async start(input: PaymentIntent): Promise<Payment> {
    const intent = validateAndCopyIntent(input);
    return inTransaction(this.pool, async (client) => {
      const resource = await readResource(client, intent.resourceId, true);
      const existing = await findPayment(client, intent.key, true);
      if (existing) {
        assertSameIntent(existing, intent);
        return existing;
      }
      if (resource.generation !== intent.generation) throw new WorkflowError('STALE_GENERATION');
      const inserted = await client.query<PaymentRow>(
        `INSERT INTO payments (idempotency_key, resource_id, generation, amount_minor, currency)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`,
        [intent.key, intent.resourceId, intent.generation, intent.amountMinor, intent.currency],
      );
      // Different resources can race for the same global key. The unique index
      // picks one winner; the loser must compare the winner's immutable intent.
      const payment = inserted.rows[0]
        ? paymentFromRow(inserted.rows[0])
        : await readPayment(client, intent.key, true);
      assertSameIntent(payment, intent);
      return payment;
    });
  }

  async complete(key: string): Promise<Payment> {
    return this.withLockedPayment(key, async (client, resource, payment) => {
      // Historical retries return history only; they never regrant entitlement.
      if (payment.state === 'completed') return payment;
      if (payment.state === 'canceled') throw new WorkflowError('PAYMENT_CANCELED');
      if (resource.generation !== payment.generation) throw new WorkflowError('STALE_GENERATION');
      if (resource.entitlementKey !== null && resource.entitlementKey !== payment.key) {
        throw new WorkflowError('ENTITLEMENT_CONFLICT');
      }
      await client.query('UPDATE resources SET entitlement_key = $1 WHERE id = $2', [key, resource.id]);
      const updated = await client.query<PaymentRow>(
        `UPDATE payments SET state = 'completed', completed_at = clock_timestamp()
         WHERE idempotency_key = $1 RETURNING *`, [key],
      );
      return paymentFromRow(updated.rows[0]!);
    });
  }

  async cancel(key: string): Promise<Payment> {
    return this.withLockedPayment(key, async (client, resource, payment) => {
      if (payment.state === 'canceled') return payment;
      // The ownership predicate matters even after a resource changes generation:
      // canceling old history must not revoke a newer payment's entitlement.
      await client.query(
        'UPDATE resources SET entitlement_key = NULL WHERE id = $1 AND entitlement_key = $2',
        [resource.id, key],
      );
      const updated = await client.query<PaymentRow>(
        `UPDATE payments SET state = 'canceled', canceled_at = clock_timestamp()
         WHERE idempotency_key = $1 RETURNING *`, [key],
      );
      return paymentFromRow(updated.rows[0]!);
    });
  }

  async advanceGeneration(id: string, expectedGeneration: number): Promise<Resource> {
    requireKey(id);
    if (!positiveInteger(expectedGeneration)) throw new WorkflowError('INVALID_INTENT');
    return inTransaction(this.pool, async (client) => {
      const resource = await readResource(client, id, true);
      if (resource.generation !== expectedGeneration) throw new WorkflowError('STALE_GENERATION');
      const updated = await client.query<ResourceRow>(
        `UPDATE resources SET generation = generation + 1, entitlement_key = NULL
         WHERE id = $1 RETURNING *`, [id],
      );
      return resourceFromRow(updated.rows[0]!);
    });
  }

  async getResource(id: string): Promise<Resource> {
    requireKey(id);
    return readResource(this.pool, id);
  }
  async getPayment(key: string): Promise<Payment> {
    requireKey(key);
    return readPayment(this.pool, key);
  }

  private async withLockedPayment<T>(
    key: string,
    operation: (client: PoolClient, resource: Resource, payment: Payment) => Promise<T>,
  ): Promise<T> {
    requireKey(key);
    return inTransaction(this.pool, async (client) => {
      // This read finds the immutable resource ID, without locking the payment.
      // Every mutation locks resource first, then rereads/locks the payment.
      const snapshot = await readPayment(client, key);
      const resource = await readResource(client, snapshot.resourceId, true);
      const payment = await readPayment(client, key, true);
      return operation(client, resource, payment);
    });
  }
}

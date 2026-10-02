import type { Pool, PoolClient } from 'pg';

// One checked-out connection owns every statement, including COMMIT. Never put
// external effects in this callback: PostgreSQL cannot roll them back.
export async function inTransaction<T>(pool: Pool, operation: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let committing = false;
  let discard: Error | undefined;
  try {
    await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    const result = await operation(client);
    committing = true;
    await client.query('COMMIT');
    return result;
  } catch (error) {
    // A lost COMMIT acknowledgement has an unknown outcome. Discard the
    // connection and let callers reconcile by key; do not blindly replay.
    if (committing) discard = error instanceof Error ? error : new Error(String(error));
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      discard = rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError));
      throw new AggregateError([error, rollbackError], 'Transaction failed; rollback could not be confirmed');
    }
    throw error;
  } finally {
    client.release(discard);
  }
}

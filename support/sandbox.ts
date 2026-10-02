import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { Pool } from 'pg';

// Tests and demos only create/drop their own random schema. No default database,
// .env loading, table truncation outside that schema, or external service calls.
export async function createSandbox() {
  const connectionString = process.env.TEST_DATABASE_URL;
  if (!connectionString) throw new Error('Set TEST_DATABASE_URL to an isolated local PostgreSQL database.');
  const address = new URL(connectionString);
  if (!['postgres:', 'postgresql:'].includes(address.protocol) ||
      !['localhost', '127.0.0.1', '[::1]'].includes(address.hostname) || address.search || address.hash) {
    throw new Error('Use a loopback PostgreSQL URL without query parameters or a fragment.');
  }
  const schema = `payment_example_${randomUUID().replaceAll('-', '')}`;
  const admin = new Pool({ connectionString, max: 1 });
  // Only generated alphanumeric identifiers are interpolated. Business values
  // are always query parameters; PostgreSQL cannot parameterize identifiers.
  const pool = new Pool({
    connectionString, max: 8, application_name: schema,
    options: `-c search_path=${schema} -c statement_timeout=10000 -c lock_timeout=5000`,
  });
  try {
    await admin.query(`CREATE SCHEMA ${schema}`);
    await pool.query(await readFile(new URL('../schema.sql', import.meta.url), 'utf8'));
  } catch (error) {
    await pool.end();
    try { await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`); }
    finally { await admin.end(); }
    throw error;
  }
  return {
    pool,
    schema,
    async waitingConnections() {
      const result = await admin.query<{ pid: number }>(
        `SELECT pid FROM pg_stat_activity
         WHERE application_name = $1 AND cardinality(pg_blocking_pids(pid)) > 0`, [schema],
      );
      return result.rows.map(({ pid }) => pid);
    },
    async close() {
      await pool.end();
      try { await admin.query(`DROP SCHEMA ${schema} CASCADE`); }
      finally { await admin.end(); }
    },
  };
}

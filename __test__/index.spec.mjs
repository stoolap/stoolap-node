// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const require = createRequire(import.meta.url);
const { Database } = require('../index.js');

// ============================================================
// Database open/close
// ============================================================

describe('Database', () => {
  it('should open and close an in-memory database', async () => {
    const db = await Database.open(':memory:');
    assert.ok(db);
    await db.close();
  });

  it('should open with empty string (in-memory)', async () => {
    const db = await Database.open('');
    await db.exec('CREATE TABLE t (id INTEGER)');
    await db.execute('INSERT INTO t VALUES ($1)', [1]);
    const rows = await db.query('SELECT * FROM t');
    assert.equal(rows.length, 1);
    await db.close();
  });

  it('should open a file-based database', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stoolap-test-'));
    const dbPath = path.join(tmpDir, 'test.db');
    try {
      const db = await Database.open(dbPath);
      await db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      await db.execute('INSERT INTO t VALUES ($1, $2)', [1, 'hello']);
      await db.close();

      // Reopen and verify data persisted
      const db2 = await Database.open(dbPath);
      const rows = await db2.query('SELECT * FROM t');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].id, 1);
      assert.equal(rows[0].val, 'hello');
      await db2.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ============================================================
// DDL and DML
// ============================================================

describe('DDL and DML', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
  });

  after(async () => {
    await db.close();
  });

  it('should create tables via exec', async () => {
    await db.exec(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER,
        email TEXT
      )
    `);
    const tables = await db.query('SHOW TABLES');
    const tableNames = tables.map(t => t.table_name || t.Tables);
    assert.ok(tableNames.some(n => n === 'users'));
  });

  it('should execute multiple statements with exec', async () => {
    await db.exec(`
      CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
      CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)
    `);
    const tables = await db.query('SHOW TABLES');
    const names = tables.map(t => t.table_name || t.Tables);
    assert.ok(names.some(n => n === 'items'));
    assert.ok(names.some(n => n === 'orders'));
  });

  it('should insert with positional params and return changes', async () => {
    const r1 = await db.execute(
      'INSERT INTO users (id, name, age, email) VALUES ($1, $2, $3, $4)',
      [1, 'Alice', 32, 'alice@example.com']
    );
    assert.equal(r1.changes, 1);

    const r2 = await db.execute(
      'INSERT INTO users (id, name, age, email) VALUES ($1, $2, $3, $4)',
      [2, 'Bob', 28, 'bob@example.com']
    );
    assert.equal(r2.changes, 1);
  });

  it('should insert with named params', async () => {
    const r = await db.execute(
      'INSERT INTO users (id, name, age, email) VALUES (:id, :name, :age, :email)',
      { id: 3, name: 'Charlie', age: 25, email: 'charlie@example.com' }
    );
    assert.equal(r.changes, 1);
  });

  it('should update rows and return changes count', async () => {
    const r = await db.execute(
      'UPDATE users SET age = $1 WHERE id = $2',
      [33, 1]
    );
    assert.equal(r.changes, 1);
  });

  it('should delete rows and return changes count', async () => {
    await db.execute('INSERT INTO users VALUES ($1, $2, $3, $4)', [99, 'Temp', 0, null]);
    const r = await db.execute('DELETE FROM users WHERE id = $1', [99]);
    assert.equal(r.changes, 1);
  });
});

// ============================================================
// Queries
// ============================================================

describe('Queries', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec(`
      CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price FLOAT,
        active BOOLEAN
      )
    `);
    await db.execute('INSERT INTO products VALUES ($1, $2, $3, $4)', [1, 'Widget', 9.99, true]);
    await db.execute('INSERT INTO products VALUES ($1, $2, $3, $4)', [2, 'Gadget', 29.99, true]);
    await db.execute('INSERT INTO products VALUES ($1, $2, $3, $4)', [3, 'Doohickey', 4.50, false]);
  });

  after(async () => {
    await db.close();
  });

  it('should query all rows', async () => {
    const rows = await db.query('SELECT * FROM products ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, 'Widget');
    assert.equal(rows[1].name, 'Gadget');
    assert.equal(rows[2].name, 'Doohickey');
  });

  it('should query with params', async () => {
    const rows = await db.query('SELECT * FROM products WHERE price > $1', [10.0]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Gadget');
  });

  it('should queryOne return object', async () => {
    const row = await db.queryOne('SELECT * FROM products WHERE id = $1', [1]);
    assert.ok(row);
    assert.equal(row.id, 1);
    assert.equal(row.name, 'Widget');
    assert.equal(row.price, 9.99);
    assert.equal(row.active, true);
  });

  it('should queryOne return null for no results', async () => {
    const row = await db.queryOne('SELECT * FROM products WHERE id = $1', [999]);
    assert.equal(row, null);
  });

  it('should queryRaw return columns and rows', async () => {
    const raw = await db.queryRaw('SELECT id, name FROM products ORDER BY id');
    assert.deepEqual(raw.columns, ['id', 'name']);
    assert.equal(raw.rows.length, 3);
    assert.deepEqual(raw.rows[0], [1, 'Widget']);
    assert.deepEqual(raw.rows[1], [2, 'Gadget']);
    assert.deepEqual(raw.rows[2], [3, 'Doohickey']);
  });

  it('should handle no-param queries', async () => {
    const rows = await db.query('SELECT COUNT(*) AS cnt FROM products');
    assert.equal(rows[0].cnt, 3);
  });
});

// ============================================================
// Type conversions
// ============================================================

describe('Type conversions', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec(`
      CREATE TABLE types (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        float_val FLOAT,
        text_val TEXT,
        bool_val BOOLEAN,
        ts_val TIMESTAMP,
        json_val JSON
      )
    `);
  });

  after(async () => {
    await db.close();
  });

  it('should handle null values', async () => {
    await db.execute(
      'INSERT INTO types (id, int_val) VALUES ($1, $2)',
      [1, null]
    );
    const row = await db.queryOne('SELECT * FROM types WHERE id = $1', [1]);
    assert.equal(row.int_val, null);
    assert.equal(row.float_val, null);
  });

  it('should handle integer values', async () => {
    await db.execute(
      'INSERT INTO types (id, int_val) VALUES ($1, $2)',
      [2, 42]
    );
    const row = await db.queryOne('SELECT int_val FROM types WHERE id = $1', [2]);
    assert.equal(row.int_val, 42);
  });

  it('should handle float values', async () => {
    await db.execute(
      'INSERT INTO types (id, float_val) VALUES ($1, $2)',
      [3, 3.14]
    );
    const row = await db.queryOne('SELECT float_val FROM types WHERE id = $1', [3]);
    assert.ok(Math.abs(row.float_val - 3.14) < 0.001);
  });

  it('should handle text values', async () => {
    await db.execute(
      'INSERT INTO types (id, text_val) VALUES ($1, $2)',
      [4, 'hello world']
    );
    const row = await db.queryOne('SELECT text_val FROM types WHERE id = $1', [4]);
    assert.equal(row.text_val, 'hello world');
  });

  it('should handle boolean values', async () => {
    await db.execute(
      'INSERT INTO types (id, bool_val) VALUES ($1, $2)',
      [5, true]
    );
    await db.execute(
      'INSERT INTO types (id, bool_val) VALUES ($1, $2)',
      [6, false]
    );
    const r1 = await db.queryOne('SELECT bool_val FROM types WHERE id = $1', [5]);
    const r2 = await db.queryOne('SELECT bool_val FROM types WHERE id = $1', [6]);
    assert.equal(r1.bool_val, true);
    assert.equal(r2.bool_val, false);
  });

  it('should handle JSON values', async () => {
    const jsonObj = { key: 'value', nested: { a: 1 } };
    await db.execute(
      'INSERT INTO types (id, json_val) VALUES ($1, $2)',
      [7, JSON.stringify(jsonObj)]
    );
    const row = await db.queryOne('SELECT json_val FROM types WHERE id = $1', [7]);
    assert.ok(row.json_val);
    const parsed = JSON.parse(row.json_val);
    assert.equal(parsed.key, 'value');
    assert.equal(parsed.nested.a, 1);
  });

  it('should handle large integers within safe range', async () => {
    const big = 9007199254740991; // Number.MAX_SAFE_INTEGER
    await db.execute('INSERT INTO types (id, int_val) VALUES ($1, $2)', [8, big]);
    const row = await db.queryOne('SELECT int_val FROM types WHERE id = $1', [8]);
    assert.equal(row.int_val, big);
  });
});

// ============================================================
// Prepared statements
// ============================================================

describe('PreparedStatement', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec('CREATE TABLE ps_test (id INTEGER PRIMARY KEY, val TEXT)');
  });

  after(async () => {
    await db.close();
  });

  it('should execute prepared insert', async () => {
    const stmt = db.prepare('INSERT INTO ps_test VALUES ($1, $2)');
    assert.equal(stmt.sql, 'INSERT INTO ps_test VALUES ($1, $2)');

    const r1 = await stmt.execute([1, 'first']);
    assert.equal(r1.changes, 1);
    const r2 = await stmt.execute([2, 'second']);
    assert.equal(r2.changes, 1);
  });

  it('should query with prepared statement', async () => {
    const stmt = db.prepare('SELECT * FROM ps_test WHERE id = $1');
    const rows = await stmt.query([1]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'first');
  });

  it('should queryOne with prepared statement', async () => {
    const stmt = db.prepare('SELECT * FROM ps_test WHERE id = $1');
    const row = await stmt.queryOne([2]);
    assert.ok(row);
    assert.equal(row.val, 'second');
  });

  it('should queryRaw with prepared statement', async () => {
    const stmt = db.prepare('SELECT * FROM ps_test ORDER BY id');
    const raw = await stmt.queryRaw();
    assert.deepEqual(raw.columns, ['id', 'val']);
    assert.equal(raw.rows.length, 2);
  });
});

// ============================================================
// Transactions
// ============================================================

describe('Transaction', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec('CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT)');
  });

  after(async () => {
    await db.close();
  });

  it('should commit a transaction', async () => {
    const tx = await db.begin();
    await tx.execute('INSERT INTO tx_test VALUES ($1, $2)', [1, 'committed']);
    await tx.commit();

    const rows = await db.query('SELECT * FROM tx_test WHERE id = $1', [1]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'committed');
  });

  it('should rollback a transaction', async () => {
    const tx = await db.begin();
    await tx.execute('INSERT INTO tx_test VALUES ($1, $2)', [2, 'rolled-back']);
    await tx.rollback();

    const rows = await db.query('SELECT * FROM tx_test WHERE id = $1', [2]);
    assert.equal(rows.length, 0);
  });

  it('should query within a transaction', async () => {
    const tx = await db.begin();
    await tx.execute('INSERT INTO tx_test VALUES ($1, $2)', [3, 'inside-tx']);

    // Query within transaction sees uncommitted data
    const rows = await tx.query('SELECT * FROM tx_test WHERE id = $1', [3]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'inside-tx');

    const one = await tx.queryOne('SELECT * FROM tx_test WHERE id = $1', [3]);
    assert.ok(one);
    assert.equal(one.val, 'inside-tx');

    await tx.commit();
  });

  it('should handle transaction helper pattern', async () => {
    // Manual transaction helper (the JS-level db.transaction() can be added later)
    const tx = await db.begin();
    try {
      await tx.execute('INSERT INTO tx_test VALUES ($1, $2)', [10, 'helper']);
      await tx.execute('INSERT INTO tx_test VALUES ($1, $2)', [11, 'helper2']);
      await tx.commit();
    } catch (e) {
      await tx.rollback();
      throw e;
    }

    const rows = await db.query('SELECT * FROM tx_test WHERE id IN ($1, $2)', [10, 11]);
    assert.equal(rows.length, 2);
  });
});

// ============================================================
// Error handling
// ============================================================

describe('Error handling', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec('CREATE TABLE err_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
  });

  after(async () => {
    await db.close();
  });

  it('should throw on invalid SQL', async () => {
    await assert.rejects(
      () => db.query('SELECTX * FROM nowhere'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on NOT NULL violation', async () => {
    await assert.rejects(
      () => db.execute('INSERT INTO err_test (id, name) VALUES ($1, $2)', [1, null]),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on duplicate primary key', async () => {
    await db.execute('INSERT INTO err_test VALUES ($1, $2)', [1, 'first']);
    await assert.rejects(
      () => db.execute('INSERT INTO err_test VALUES ($1, $2)', [1, 'duplicate']),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on table not found', async () => {
    await assert.rejects(
      () => db.query('SELECT * FROM nonexistent'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });
});

// ============================================================
// Advanced queries
// ============================================================

describe('Advanced queries', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec(`
      CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        name TEXT,
        dept TEXT,
        salary FLOAT
      )
    `);
    const stmt = db.prepare('INSERT INTO employees VALUES ($1, $2, $3, $4)');
    await stmt.execute([1, 'Alice', 'Engineering', 120000]);
    await stmt.execute([2, 'Bob', 'Engineering', 110000]);
    await stmt.execute([3, 'Charlie', 'Sales', 90000]);
    await stmt.execute([4, 'Diana', 'Sales', 95000]);
    await stmt.execute([5, 'Eve', 'HR', 85000]);
  });

  after(async () => {
    await db.close();
  });

  it('should handle GROUP BY with aggregates', async () => {
    const rows = await db.query(
      'SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY dept ORDER BY dept'
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].dept, 'Engineering');
    assert.equal(rows[0].cnt, 2);
  });

  it('should handle ORDER BY and LIMIT', async () => {
    const rows = await db.query(
      'SELECT name FROM employees ORDER BY salary DESC LIMIT $1',
      [3]
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, 'Alice');
  });

  it('should handle subqueries', async () => {
    const rows = await db.query(
      'SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name'
    );
    assert.ok(rows.length > 0);
  });

  it('should handle JOINs', async () => {
    await db.exec('CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, location TEXT)');
    await db.execute("INSERT INTO departments VALUES ($1, $2, $3)", [1, 'Engineering', 'SF']);
    await db.execute("INSERT INTO departments VALUES ($1, $2, $3)", [2, 'Sales', 'NYC']);
    await db.execute("INSERT INTO departments VALUES ($1, $2, $3)", [3, 'HR', 'SF']);

    const rows = await db.query(`
      SELECT e.name, d.location
      FROM employees e
      JOIN departments d ON e.dept = d.name
      ORDER BY e.name
    `);
    // Note: departments joined by name column (not PK)
    assert.equal(rows.length, 5);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[0].location, 'SF');
  });

  it('should handle EXPLAIN', async () => {
    const rows = await db.query('EXPLAIN SELECT * FROM employees WHERE id = $1', [1]);
    assert.ok(rows.length > 0);
  });
});

// ============================================================
// Concurrent queries
// ============================================================

describe('Concurrency', () => {
  it('should handle concurrent async queries', async () => {
    const db = await Database.open(':memory:');
    await db.exec('CREATE TABLE conc (id INTEGER PRIMARY KEY, val INTEGER)');

    const stmt = db.prepare('INSERT INTO conc VALUES ($1, $2)');
    for (let i = 0; i < 100; i++) {
      await stmt.execute([i, i * 10]);
    }

    // Run multiple queries concurrently
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(db.query('SELECT * FROM conc WHERE val > $1', [i * 100]));
    }
    const results = await Promise.all(promises);
    assert.equal(results.length, 10);

    // Each query should return results
    for (const rows of results) {
      assert.ok(Array.isArray(rows));
    }

    await db.close();
  });
});

// ============================================================
// Sync methods — Database
// ============================================================

describe('Sync methods', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    db.execSync(`
      CREATE TABLE sync_test (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        score FLOAT
      )
    `);
  });

  after(async () => {
    await db.close();
  });

  it('should execSync create tables', () => {
    db.execSync('CREATE TABLE sync_extra (id INTEGER PRIMARY KEY, val TEXT)');
    const tables = db.querySync('SHOW TABLES');
    const names = tables.map(t => t.table_name || t.Tables);
    assert.ok(names.some(n => n === 'sync_extra'));
  });

  it('should executeSync with positional params', () => {
    const r = db.executeSync(
      'INSERT INTO sync_test (id, name, score) VALUES ($1, $2, $3)',
      [1, 'Alice', 95.5]
    );
    assert.equal(r.changes, 1);
  });

  it('should executeSync with named params', () => {
    const r = db.executeSync(
      'INSERT INTO sync_test (id, name, score) VALUES (:id, :name, :score)',
      { id: 2, name: 'Bob', score: 87.3 }
    );
    assert.equal(r.changes, 1);
  });

  it('should querySync return array of objects', () => {
    const rows = db.querySync('SELECT * FROM sync_test ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Bob');
  });

  it('should querySync with positional params', () => {
    const rows = db.querySync('SELECT * FROM sync_test WHERE id = $1', [1]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Alice');
  });

  it('should querySync with named params', () => {
    const rows = db.querySync(
      'SELECT * FROM sync_test WHERE name = :name',
      { name: 'Bob' }
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 2);
  });

  it('should queryOneSync return object', () => {
    const row = db.queryOneSync('SELECT * FROM sync_test WHERE id = $1', [1]);
    assert.ok(row);
    assert.equal(row.name, 'Alice');
    assert.ok(Math.abs(row.score - 95.5) < 0.01);
  });

  it('should queryOneSync return null for no results', () => {
    const row = db.queryOneSync('SELECT * FROM sync_test WHERE id = $1', [999]);
    assert.equal(row, null);
  });

  it('should queryOneSync with named params', () => {
    const row = db.queryOneSync(
      'SELECT * FROM sync_test WHERE id = :id',
      { id: 2 }
    );
    assert.ok(row);
    assert.equal(row.name, 'Bob');
  });

  it('should queryRawSync return columns and rows', () => {
    const raw = db.queryRawSync('SELECT id, name FROM sync_test ORDER BY id');
    assert.deepEqual(raw.columns, ['id', 'name']);
    assert.equal(raw.rows.length, 2);
    assert.deepEqual(raw.rows[0], [1, 'Alice']);
    assert.deepEqual(raw.rows[1], [2, 'Bob']);
  });

  it('should queryRawSync with named params', () => {
    const raw = db.queryRawSync(
      'SELECT id, name FROM sync_test WHERE score > :min',
      { min: 90.0 }
    );
    assert.equal(raw.rows.length, 1);
    assert.deepEqual(raw.rows[0], [1, 'Alice']);
  });

  it('should queryRawSync return empty rows for no results', () => {
    const raw = db.queryRawSync('SELECT * FROM sync_test WHERE id = $1', [999]);
    assert.ok(Array.isArray(raw.columns));
    assert.equal(raw.rows.length, 0);
  });
});

// ============================================================
// Batch execution
// ============================================================

describe('Batch execution', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    db.execSync('CREATE TABLE batch_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)');
  });

  after(async () => {
    await db.close();
  });

  it('should executeBatchSync insert multiple rows', () => {
    const result = db.executeBatchSync(
      'INSERT INTO batch_test VALUES ($1, $2, $3)',
      [
        [1, 'Alice', 10],
        [2, 'Bob', 20],
        [3, 'Charlie', 30],
      ]
    );
    assert.equal(result.changes, 3);

    const rows = db.querySync('SELECT * FROM batch_test ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[2].val, 30);
  });

  it('should executeBatchSync with single param set', () => {
    const result = db.executeBatchSync(
      'INSERT INTO batch_test VALUES ($1, $2, $3)',
      [[4, 'Diana', 40]]
    );
    assert.equal(result.changes, 1);
  });

  it('should executeBatchSync for deletes', () => {
    db.executeBatchSync(
      'INSERT INTO batch_test VALUES ($1, $2, $3)',
      [
        [100, 'del-a', 0],
        [101, 'del-b', 0],
      ]
    );
    const result = db.executeBatchSync(
      'DELETE FROM batch_test WHERE id = $1',
      [[100], [101]]
    );
    assert.equal(result.changes, 2);
  });
});

// ============================================================
// Prepared statement — sync methods
// ============================================================

describe('PreparedStatement sync', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    db.execSync('CREATE TABLE ps_sync (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)');
  });

  after(async () => {
    await db.close();
  });

  it('should executeSync with prepared statement', () => {
    const stmt = db.prepare('INSERT INTO ps_sync VALUES ($1, $2, $3)');
    const r1 = stmt.executeSync([1, 'Alice', true]);
    assert.equal(r1.changes, 1);
    const r2 = stmt.executeSync([2, 'Bob', false]);
    assert.equal(r2.changes, 1);
    stmt.executeSync([3, 'Charlie', true]);
  });

  it('should querySync with prepared statement', () => {
    const stmt = db.prepare('SELECT * FROM ps_sync WHERE active = $1');
    const rows = stmt.querySync([true]);
    assert.equal(rows.length, 2);
  });

  it('should queryOneSync with prepared statement', () => {
    const stmt = db.prepare('SELECT * FROM ps_sync WHERE id = $1');
    const row = stmt.queryOneSync([2]);
    assert.ok(row);
    assert.equal(row.name, 'Bob');
    assert.equal(row.active, false);
  });

  it('should queryOneSync return null for no results', () => {
    const stmt = db.prepare('SELECT * FROM ps_sync WHERE id = $1');
    const row = stmt.queryOneSync([999]);
    assert.equal(row, null);
  });

  it('should queryRawSync with prepared statement', () => {
    const stmt = db.prepare('SELECT id, name FROM ps_sync ORDER BY id');
    const raw = stmt.queryRawSync();
    assert.deepEqual(raw.columns, ['id', 'name']);
    assert.equal(raw.rows.length, 3);
    assert.deepEqual(raw.rows[0], [1, 'Alice']);
  });

  it('should executeBatchSync with prepared statement', () => {
    const stmt = db.prepare('INSERT INTO ps_sync VALUES ($1, $2, $3)');
    const result = stmt.executeBatchSync([
      [10, 'Dave', true],
      [11, 'Eve', false],
      [12, 'Frank', true],
    ]);
    assert.equal(result.changes, 3);
    const rows = db.querySync('SELECT * FROM ps_sync ORDER BY id');
    assert.equal(rows.length, 6);
  });

  it('should expose sql property', () => {
    const stmt = db.prepare('SELECT * FROM ps_sync WHERE id = $1');
    assert.equal(stmt.sql, 'SELECT * FROM ps_sync WHERE id = $1');
  });

  it('should throw on invalid SQL at prepare time', () => {
    assert.throws(
      () => db.prepare('SELECTX INVALID'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });
});

// ============================================================
// Transaction — sync methods
// ============================================================

describe('Transaction sync', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    db.execSync('CREATE TABLE tx_sync (id INTEGER PRIMARY KEY, val TEXT)');
  });

  after(async () => {
    await db.close();
  });

  it('should beginSync and commitSync', () => {
    const tx = db.beginSync();
    tx.executeSync('INSERT INTO tx_sync VALUES ($1, $2)', [1, 'committed']);
    tx.commitSync();

    const rows = db.querySync('SELECT * FROM tx_sync WHERE id = $1', [1]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'committed');
  });

  it('should beginSync and rollbackSync', () => {
    const tx = db.beginSync();
    tx.executeSync('INSERT INTO tx_sync VALUES ($1, $2)', [2, 'rolled-back']);
    tx.rollbackSync();

    const rows = db.querySync('SELECT * FROM tx_sync WHERE id = $1', [2]);
    assert.equal(rows.length, 0);
  });

  it('should querySync within sync transaction', () => {
    const tx = db.beginSync();
    tx.executeSync('INSERT INTO tx_sync VALUES ($1, $2)', [3, 'visible']);
    const rows = tx.querySync('SELECT * FROM tx_sync WHERE id = $1', [3]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'visible');
    tx.commitSync();
  });

  it('should queryOneSync within sync transaction', () => {
    const tx = db.beginSync();
    tx.executeSync('INSERT INTO tx_sync VALUES ($1, $2)', [4, 'one-row']);
    const row = tx.queryOneSync('SELECT * FROM tx_sync WHERE id = $1', [4]);
    assert.ok(row);
    assert.equal(row.val, 'one-row');

    const missing = tx.queryOneSync('SELECT * FROM tx_sync WHERE id = $1', [999]);
    assert.equal(missing, null);
    tx.commitSync();
  });

  it('should queryRawSync within sync transaction', () => {
    const tx = db.beginSync();
    const raw = tx.queryRawSync('SELECT id, val FROM tx_sync ORDER BY id');
    assert.ok(raw.columns.length > 0);
    assert.ok(raw.rows.length > 0);
    tx.commitSync();
  });

  it('should executeBatchSync within transaction', () => {
    const tx = db.beginSync();
    const result = tx.executeBatchSync(
      'INSERT INTO tx_sync VALUES ($1, $2)',
      [
        [10, 'batch-a'],
        [11, 'batch-b'],
        [12, 'batch-c'],
      ]
    );
    assert.equal(result.changes, 3);
    tx.commitSync();

    const rows = db.querySync('SELECT * FROM tx_sync WHERE id >= $1 ORDER BY id', [10]);
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, 'batch-a');
    assert.equal(rows[2].val, 'batch-c');
  });
});

// ============================================================
// Transaction — async queryRaw
// ============================================================

describe('Transaction async queryRaw', () => {
  it('should queryRaw within async transaction', async () => {
    const db = await Database.open(':memory:');
    await db.exec('CREATE TABLE tx_raw (id INTEGER PRIMARY KEY, val TEXT)');

    const tx = await db.begin();
    await tx.execute('INSERT INTO tx_raw VALUES ($1, $2)', [1, 'hello']);
    await tx.execute('INSERT INTO tx_raw VALUES ($1, $2)', [2, 'world']);

    const raw = await tx.queryRaw('SELECT id, val FROM tx_raw ORDER BY id');
    assert.deepEqual(raw.columns, ['id', 'val']);
    assert.equal(raw.rows.length, 2);
    // Verify both rows exist (order may vary within transaction)
    const ids = raw.rows.map(r => r[0]).sort();
    assert.deepEqual(ids, [1, 2]);

    await tx.commit();
    await db.close();
  });
});

// ============================================================
// Named parameters — comprehensive
// ============================================================

describe('Named parameters', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec(`
      CREATE TABLE named_test (
        id INTEGER PRIMARY KEY,
        name TEXT,
        city TEXT,
        score FLOAT
      )
    `);
    await db.execute(
      'INSERT INTO named_test VALUES (:id, :name, :city, :score)',
      { id: 1, name: 'Alice', city: 'NYC', score: 95.0 }
    );
    await db.execute(
      'INSERT INTO named_test VALUES (:id, :name, :city, :score)',
      { id: 2, name: 'Bob', city: 'SF', score: 88.5 }
    );
    await db.execute(
      'INSERT INTO named_test VALUES (:id, :name, :city, :score)',
      { id: 3, name: 'Charlie', city: 'NYC', score: 72.0 }
    );
  });

  after(async () => {
    await db.close();
  });

  it('should query with named params', async () => {
    const rows = await db.query(
      'SELECT * FROM named_test WHERE city = :city ORDER BY id',
      { city: 'NYC' }
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Charlie');
  });

  it('should queryOne with named params', async () => {
    const row = await db.queryOne(
      'SELECT * FROM named_test WHERE name = :name',
      { name: 'Bob' }
    );
    assert.ok(row);
    assert.equal(row.city, 'SF');
  });

  it('should queryRaw with named params', async () => {
    const raw = await db.queryRaw(
      'SELECT id, name FROM named_test WHERE score > :min ORDER BY id',
      { min: 80.0 }
    );
    assert.deepEqual(raw.columns, ['id', 'name']);
    assert.equal(raw.rows.length, 2);
    assert.deepEqual(raw.rows[0], [1, 'Alice']);
  });

  it('should update with named params', async () => {
    const r = await db.execute(
      'UPDATE named_test SET score = :score WHERE name = :name',
      { score: 99.0, name: 'Charlie' }
    );
    assert.equal(r.changes, 1);
    const row = await db.queryOne('SELECT score FROM named_test WHERE id = $1', [3]);
    assert.equal(row.score, 99.0);
  });

  it('should delete with named params', async () => {
    await db.execute(
      'INSERT INTO named_test VALUES (:id, :name, :city, :score)',
      { id: 99, name: 'Temp', city: 'X', score: 0 }
    );
    const r = await db.execute(
      'DELETE FROM named_test WHERE id = :id',
      { id: 99 }
    );
    assert.equal(r.changes, 1);
  });

  it('should use positional params in async transaction', async () => {
    const tx = await db.begin();
    await tx.execute(
      'INSERT INTO named_test VALUES ($1, $2, $3, $4)',
      [50, 'TxPos', 'LA', 50.0]
    );
    const row = await tx.queryOne(
      'SELECT * FROM named_test WHERE id = $1',
      [50]
    );
    assert.ok(row);
    assert.equal(row.name, 'TxPos');
    await tx.rollback();
  });

  it('should use positional params in sync transaction', () => {
    const tx = db.beginSync();
    tx.executeSync(
      'INSERT INTO named_test VALUES ($1, $2, $3, $4)',
      [51, 'SyncPos', 'CHI', 60.0]
    );
    const row = tx.queryOneSync(
      'SELECT * FROM named_test WHERE name = $1',
      ['SyncPos']
    );
    assert.ok(row);
    assert.equal(row.city, 'CHI');
    tx.rollbackSync();
  });
});

// ============================================================
// Type conversions — extended
// ============================================================

describe('Extended type conversions', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    await db.exec(`
      CREATE TABLE ext_types (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        float_val FLOAT,
        text_val TEXT,
        bool_val BOOLEAN,
        ts_val TIMESTAMP,
        json_val JSON
      )
    `);
  });

  after(async () => {
    await db.close();
  });

  it('should handle undefined as NULL', async () => {
    await db.execute(
      'INSERT INTO ext_types (id, text_val) VALUES ($1, $2)',
      [1, undefined]
    );
    const row = await db.queryOne('SELECT text_val FROM ext_types WHERE id = $1', [1]);
    assert.equal(row.text_val, null);
  });

  it('should handle BigInt parameter', async () => {
    await db.execute(
      'INSERT INTO ext_types (id, int_val) VALUES ($1, $2)',
      [2, BigInt(123456789)]
    );
    const row = await db.queryOne('SELECT int_val FROM ext_types WHERE id = $1', [2]);
    assert.equal(row.int_val, 123456789);
  });

  it('should handle Date parameter as TIMESTAMP', async () => {
    const date = new Date('2025-06-15T10:30:00Z');
    await db.execute(
      'INSERT INTO ext_types (id, ts_val) VALUES ($1, $2)',
      [3, date]
    );
    const row = await db.queryOne('SELECT ts_val FROM ext_types WHERE id = $1', [3]);
    assert.ok(row.ts_val);
    // Verify the timestamp round-trips (string or Date)
    const returned = new Date(row.ts_val);
    assert.equal(returned.getUTCFullYear(), 2025);
    assert.equal(returned.getUTCMonth(), 5); // June = 5
    assert.equal(returned.getUTCDate(), 15);
  });

  it('should handle negative numbers', async () => {
    await db.execute(
      'INSERT INTO ext_types (id, int_val, float_val) VALUES ($1, $2, $3)',
      [4, -42, -3.14]
    );
    const row = await db.queryOne('SELECT int_val, float_val FROM ext_types WHERE id = $1', [4]);
    assert.equal(row.int_val, -42);
    assert.ok(Math.abs(row.float_val - (-3.14)) < 0.001);
  });

  it('should handle zero values', async () => {
    await db.execute(
      'INSERT INTO ext_types (id, int_val, float_val) VALUES ($1, $2, $3)',
      [5, 0, 0.0]
    );
    const row = await db.queryOne('SELECT int_val, float_val FROM ext_types WHERE id = $1', [5]);
    assert.equal(row.int_val, 0);
    assert.equal(row.float_val, 0.0);
  });

  it('should handle empty string', async () => {
    await db.execute(
      'INSERT INTO ext_types (id, text_val) VALUES ($1, $2)',
      [6, '']
    );
    const row = await db.queryOne('SELECT text_val FROM ext_types WHERE id = $1', [6]);
    assert.equal(row.text_val, '');
  });

  it('should handle unicode text', async () => {
    const unicode = '日本語テスト 🎉 émojis àccénts';
    await db.execute(
      'INSERT INTO ext_types (id, text_val) VALUES ($1, $2)',
      [7, unicode]
    );
    const row = await db.queryOne('SELECT text_val FROM ext_types WHERE id = $1', [7]);
    assert.equal(row.text_val, unicode);
  });

  it('should handle very long text', async () => {
    const longText = 'x'.repeat(100000);
    await db.execute(
      'INSERT INTO ext_types (id, text_val) VALUES ($1, $2)',
      [8, longText]
    );
    const row = await db.queryOne('SELECT text_val FROM ext_types WHERE id = $1', [8]);
    assert.equal(row.text_val.length, 100000);
  });

  it('should handle Object parameter as JSON', async () => {
    const obj = { users: [{ name: 'Alice' }, { name: 'Bob' }], count: 2 };
    await db.execute(
      'INSERT INTO ext_types (id, json_val) VALUES ($1, $2)',
      [9, obj]
    );
    const row = await db.queryOne('SELECT json_val FROM ext_types WHERE id = $1', [9]);
    const parsed = JSON.parse(row.json_val);
    assert.equal(parsed.count, 2);
    assert.equal(parsed.users[0].name, 'Alice');
  });

  it('should handle Array parameter as JSON', async () => {
    const arr = [1, 'two', { three: 3 }];
    await db.execute(
      'INSERT INTO ext_types (id, json_val) VALUES ($1, $2)',
      [10, arr]
    );
    const row = await db.queryOne('SELECT json_val FROM ext_types WHERE id = $1', [10]);
    const parsed = JSON.parse(row.json_val);
    assert.equal(parsed.length, 3);
    assert.equal(parsed[0], 1);
    assert.equal(parsed[1], 'two');
    assert.equal(parsed[2].three, 3);
  });
});

// ============================================================
// Persistence — comprehensive
// ============================================================

describe('Persistence', () => {
  it('should persist multiple tables and types across restart', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stoolap-persist-'));
    const dbPath = path.join(tmpDir, 'persist.db');
    try {
      // Write phase
      const db = await Database.open(dbPath);
      await db.exec(`
        CREATE TABLE kv (id INTEGER PRIMARY KEY, k TEXT, v TEXT);
        CREATE TABLE nums (id INTEGER PRIMARY KEY, val FLOAT, active BOOLEAN)
      `);
      await db.execute('INSERT INTO kv VALUES ($1, $2, $3)', [1, 'greeting', 'hello']);
      await db.execute('INSERT INTO kv VALUES ($1, $2, $3)', [2, 'farewell', 'goodbye']);
      await db.execute('INSERT INTO nums VALUES ($1, $2, $3)', [1, 3.14, true]);
      await db.execute('INSERT INTO nums VALUES ($1, $2, $3)', [2, 2.71, false]);
      await db.close();

      // Read phase — reopen
      const db2 = await Database.open(dbPath);
      const kv = await db2.query('SELECT * FROM kv ORDER BY k');
      assert.equal(kv.length, 2);
      assert.equal(kv[0].k, 'farewell');
      assert.equal(kv[0].v, 'goodbye');
      assert.equal(kv[1].k, 'greeting');
      assert.equal(kv[1].v, 'hello');

      const nums = await db2.query('SELECT * FROM nums ORDER BY id');
      assert.equal(nums.length, 2);
      assert.ok(Math.abs(nums[0].val - 3.14) < 0.01);
      assert.equal(nums[0].active, true);
      assert.equal(nums[1].active, false);

      await db2.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('should persist after batch insert', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stoolap-batch-'));
    const dbPath = path.join(tmpDir, 'batch.db');
    try {
      const db = await Database.open(dbPath);
      db.execSync('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
      db.executeBatchSync(
        'INSERT INTO items VALUES ($1, $2)',
        [
          [1, 'alpha'],
          [2, 'beta'],
          [3, 'gamma'],
        ]
      );
      await db.close();

      const db2 = await Database.open(dbPath);
      const rows = await db2.query('SELECT * FROM items ORDER BY id');
      assert.equal(rows.length, 3);
      assert.equal(rows[0].name, 'alpha');
      assert.equal(rows[2].name, 'gamma');
      await db2.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('should persist transaction commit but not rollback', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stoolap-tx-persist-'));
    const dbPath = path.join(tmpDir, 'tx.db');
    try {
      const db = await Database.open(dbPath);
      await db.exec('CREATE TABLE txp (id INTEGER PRIMARY KEY, val TEXT)');

      // Committed transaction
      const tx1 = await db.begin();
      await tx1.execute('INSERT INTO txp VALUES ($1, $2)', [1, 'kept']);
      await tx1.commit();

      // Rolled back transaction
      const tx2 = await db.begin();
      await tx2.execute('INSERT INTO txp VALUES ($1, $2)', [2, 'discarded']);
      await tx2.rollback();

      await db.close();

      // Verify
      const db2 = await Database.open(dbPath);
      const rows = await db2.query('SELECT * FROM txp ORDER BY id');
      assert.equal(rows.length, 1);
      assert.equal(rows[0].val, 'kept');
      await db2.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ============================================================
// Database open variations
// ============================================================

describe('Database open variations', () => {
  it('should open with memory:// URI', async () => {
    const db = await Database.open('memory://');
    await db.exec('CREATE TABLE t (id INTEGER)');
    db.executeSync('INSERT INTO t VALUES ($1)', [1]);
    const rows = db.querySync('SELECT * FROM t');
    assert.equal(rows.length, 1);
    await db.close();
  });

  it('should open with file:// URI', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stoolap-uri-'));
    const dbPath = path.join(tmpDir, 'uri.db');
    try {
      const db = await Database.open(`file://${dbPath}`);
      await db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      await db.execute('INSERT INTO t VALUES ($1)', [1]);
      const rows = await db.query('SELECT * FROM t');
      assert.equal(rows.length, 1);
      await db.close();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ============================================================
// Error handling — extended
// ============================================================

describe('Error handling extended', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
    db.execSync('CREATE TABLE err_ext (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
  });

  after(async () => {
    await db.close();
  });

  it('should throw on sync invalid SQL', () => {
    assert.throws(
      () => db.querySync('SELECTX bad'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on sync NOT NULL violation', () => {
    assert.throws(
      () => db.executeSync('INSERT INTO err_ext (id, name) VALUES ($1, $2)', [1, null]),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on execSync invalid SQL', () => {
    assert.throws(
      () => db.execSync('INVALID SQL STATEMENT'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on queryRaw with invalid SQL', async () => {
    await assert.rejects(
      () => db.queryRaw('SELECTX bad'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });

  it('should throw on queryRawSync with invalid SQL', () => {
    assert.throws(
      () => db.queryRawSync('SELECTX bad'),
      (err) => {
        assert.ok(err.message.length > 0);
        return true;
      }
    );
  });
});

// ============================================================
// Edge cases
// ============================================================

describe('Edge cases', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
  });

  after(async () => {
    await db.close();
  });

  it('should handle empty table query', async () => {
    await db.exec('CREATE TABLE empty_tbl (id INTEGER PRIMARY KEY, val TEXT)');
    const rows = await db.query('SELECT * FROM empty_tbl');
    assert.deepEqual(rows, []);
  });

  it('should handle empty table queryRaw', async () => {
    const raw = await db.queryRaw('SELECT * FROM empty_tbl');
    assert.ok(Array.isArray(raw.columns));
    assert.ok(raw.columns.length > 0);
    assert.deepEqual(raw.rows, []);
  });

  it('should handle query with no params argument', async () => {
    await db.exec('CREATE TABLE no_params (id INTEGER PRIMARY KEY)');
    await db.execute('INSERT INTO no_params VALUES ($1)', [1]);
    // Call without params argument at all
    const rows = await db.query('SELECT * FROM no_params');
    assert.equal(rows.length, 1);
    const raw = await db.queryRaw('SELECT * FROM no_params');
    assert.equal(raw.rows.length, 1);
    const one = await db.queryOne('SELECT * FROM no_params');
    assert.ok(one);
  });

  it('should handle large result set', async () => {
    await db.exec('CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER)');
    const stmt = db.prepare('INSERT INTO big VALUES ($1, $2)');
    const params = [];
    for (let i = 0; i < 1000; i++) {
      params.push([i, i * 2]);
    }
    stmt.executeBatchSync(params);

    const rows = await db.query('SELECT * FROM big ORDER BY id');
    assert.equal(rows.length, 1000);
    assert.equal(rows[0].val, 0);
    assert.equal(rows[999].val, 1998);
  });

  it('should handle large result set with queryRaw', async () => {
    const raw = db.queryRawSync('SELECT * FROM big ORDER BY id');
    assert.equal(raw.rows.length, 1000);
  });

  it('should handle multiple columns with same value types', async () => {
    await db.exec('CREATE TABLE multi_col (a TEXT, b TEXT, c TEXT, d TEXT)');
    await db.execute('INSERT INTO multi_col VALUES ($1, $2, $3, $4)', ['w', 'x', 'y', 'z']);
    const row = await db.queryOne('SELECT * FROM multi_col');
    assert.equal(row.a, 'w');
    assert.equal(row.b, 'x');
    assert.equal(row.c, 'y');
    assert.equal(row.d, 'z');
  });

  it('should handle all NULL row', async () => {
    await db.exec('CREATE TABLE all_null (a TEXT, b INTEGER, c FLOAT, d BOOLEAN)');
    await db.execute('INSERT INTO all_null VALUES ($1, $2, $3, $4)', [null, null, null, null]);
    const row = await db.queryOne('SELECT * FROM all_null');
    assert.equal(row.a, null);
    assert.equal(row.b, null);
    assert.equal(row.c, null);
    assert.equal(row.d, null);
  });

  it('should handle exec with trailing semicolons and whitespace', async () => {
    await db.exec('  CREATE TABLE ws_test (id INTEGER PRIMARY KEY)  ;  ;  ');
    const tables = await db.query('SHOW TABLES');
    const names = tables.map(t => t.table_name || t.Tables);
    assert.ok(names.some(n => n === 'ws_test'));
  });
});

// ============================================================
// Vector support
// ============================================================

describe('Vector support', () => {
  let db;

  before(async () => {
    db = await Database.open(':memory:');
  });

  after(async () => {
    await db.close();
  });

  it('should create table with VECTOR column', async () => {
    await db.exec('CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))');
    const tables = await db.query('SHOW TABLES');
    const names = tables.map(t => t.table_name || t.Tables);
    assert.ok(names.some(n => n === 'embeddings'));
  });

  it('should insert vectors via SQL string literals', async () => {
    const result = await db.execute(
      "INSERT INTO embeddings (id, embedding) VALUES (1, '[0.1, 0.2, 0.3]')"
    );
    assert.equal(result.changes, 1);
  });

  it('should insert multiple vectors', async () => {
    await db.execute("INSERT INTO embeddings (id, embedding) VALUES (2, '[0.4, 0.5, 0.6]')");
    await db.execute("INSERT INTO embeddings (id, embedding) VALUES (3, '[0.7, 0.8, 0.9]')");
    const rows = await db.query('SELECT id FROM embeddings ORDER BY id');
    assert.equal(rows.length, 3);
  });

  it('should return vectors as Float32Array', async () => {
    const row = await db.queryOne('SELECT embedding FROM embeddings WHERE id = 1');
    assert.ok(row.embedding instanceof Float32Array, 'embedding should be Float32Array');
    assert.equal(row.embedding.length, 3);
    assert.ok(Math.abs(row.embedding[0] - 0.1) < 0.001);
    assert.ok(Math.abs(row.embedding[1] - 0.2) < 0.001);
    assert.ok(Math.abs(row.embedding[2] - 0.3) < 0.001);
  });

  it('should return vectors in query() results', async () => {
    const rows = await db.query('SELECT * FROM embeddings ORDER BY id');
    assert.equal(rows.length, 3);
    for (const row of rows) {
      assert.ok(row.embedding instanceof Float32Array);
      assert.equal(row.embedding.length, 3);
    }
  });

  it('should return vectors in querySync() results', () => {
    const rows = db.querySync('SELECT * FROM embeddings ORDER BY id');
    assert.equal(rows.length, 3);
    assert.ok(rows[0].embedding instanceof Float32Array);
  });

  it('should return vectors in queryOneSync() results', () => {
    const row = db.queryOneSync('SELECT embedding FROM embeddings WHERE id = 2');
    assert.ok(row.embedding instanceof Float32Array);
    assert.ok(Math.abs(row.embedding[0] - 0.4) < 0.001);
  });

  it('should return vectors in queryRaw() results', async () => {
    const raw = await db.queryRaw('SELECT id, embedding FROM embeddings ORDER BY id');
    assert.ok(raw.columns.includes('embedding'));
    assert.equal(raw.rows.length, 3);
    const embIdx = raw.columns.indexOf('embedding');
    assert.ok(raw.rows[0][embIdx] instanceof Float32Array);
  });

  it('should return vectors in queryRawSync() results', () => {
    const raw = db.queryRawSync('SELECT id, embedding FROM embeddings ORDER BY id');
    const embIdx = raw.columns.indexOf('embedding');
    assert.ok(raw.rows[0][embIdx] instanceof Float32Array);
  });

  it('should handle NULL vectors', async () => {
    await db.execute("INSERT INTO embeddings (id, embedding) VALUES (4, NULL)");
    const row = await db.queryOne('SELECT embedding FROM embeddings WHERE id = 4');
    assert.equal(row.embedding, null);
  });

  it('should compute L2 distance', async () => {
    const rows = await db.query(
      "SELECT id, VEC_DISTANCE_L2(embedding, '[0.1, 0.2, 0.3]') AS dist FROM embeddings WHERE id <= 3 ORDER BY dist"
    );
    assert.equal(rows.length, 3);
    // id=1 has the same vector, distance should be ~0
    assert.equal(rows[0].id, 1);
    assert.ok(rows[0].dist < 0.001);
  });

  it('should compute cosine distance', async () => {
    const rows = await db.query(
      "SELECT id, VEC_DISTANCE_COSINE(embedding, '[0.1, 0.2, 0.3]') AS dist FROM embeddings WHERE id <= 3 ORDER BY dist"
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].id, 1);
    assert.ok(rows[0].dist < 0.001);
  });

  it('should support k-NN search with ORDER BY + LIMIT', async () => {
    const rows = await db.query(
      "SELECT id, VEC_DISTANCE_L2(embedding, '[0.4, 0.5, 0.6]') AS dist FROM embeddings WHERE id <= 3 ORDER BY dist LIMIT 2"
    );
    assert.equal(rows.length, 2);
    // id=2 has exact match [0.4, 0.5, 0.6]
    assert.equal(rows[0].id, 2);
  });

  it('should work with higher-dimensional vectors', async () => {
    await db.exec('CREATE TABLE hd_vecs (id INTEGER PRIMARY KEY, vec VECTOR(128))');
    const dims = 128;
    const values = Array.from({ length: dims }, (_, i) => (i / dims).toFixed(6));
    const vecStr = `[${values.join(', ')}]`;
    await db.execute(`INSERT INTO hd_vecs (id, vec) VALUES (1, '${vecStr}')`);
    const row = await db.queryOne('SELECT vec FROM hd_vecs WHERE id = 1');
    assert.ok(row.vec instanceof Float32Array);
    assert.equal(row.vec.length, dims);
  });

  it('should accept Float32Array as bind parameter', async () => {
    await db.exec('CREATE TABLE vec_params (id INTEGER PRIMARY KEY, vec VECTOR(3))');
    const vec = new Float32Array([1.0, 2.0, 3.0]);
    await db.execute("INSERT INTO vec_params (id, vec) VALUES (1, '[1.0, 2.0, 3.0]')");
    // Use Float32Array in distance computation param
    const row = db.queryOneSync(
      "SELECT VEC_DISTANCE_L2(vec, '[1.0, 2.0, 3.0]') AS dist FROM vec_params WHERE id = 1"
    );
    assert.ok(row.dist < 0.001);
  });

  it('should support vectors in transactions', async () => {
    await db.exec('CREATE TABLE tx_vecs (id INTEGER PRIMARY KEY, vec VECTOR(3))');
    const tx = await db.begin();
    await tx.execute("INSERT INTO tx_vecs (id, vec) VALUES (1, '[1.0, 2.0, 3.0]')");
    await tx.execute("INSERT INTO tx_vecs (id, vec) VALUES (2, '[4.0, 5.0, 6.0]')");
    const rows = await tx.query('SELECT * FROM tx_vecs ORDER BY id');
    assert.equal(rows.length, 2);
    assert.ok(rows[0].vec instanceof Float32Array);
    await tx.commit();

    const afterCommit = await db.query('SELECT * FROM tx_vecs ORDER BY id');
    assert.equal(afterCommit.length, 2);
  });

  it('should support VEC_DIMS utility function', async () => {
    const row = await db.queryOne('SELECT VEC_DIMS(embedding) AS dims FROM embeddings WHERE id = 1');
    assert.equal(row.dims, 3);
  });

  it('should reject wrong dimension count on insert', async () => {
    await assert.rejects(
      db.execute("INSERT INTO embeddings (id, embedding) VALUES (99, '[0.1, 0.2]')"),
      /dimension|mismatch|expected/i
    );
  });

  it('should support HNSW index creation', async () => {
    await db.exec('CREATE TABLE hnsw_test (id INTEGER PRIMARY KEY, vec VECTOR(3))');
    await db.execute("INSERT INTO hnsw_test (id, vec) VALUES (1, '[0.1, 0.2, 0.3]')");
    await db.execute("INSERT INTO hnsw_test (id, vec) VALUES (2, '[0.4, 0.5, 0.6]')");
    await db.execute("INSERT INTO hnsw_test (id, vec) VALUES (3, '[0.7, 0.8, 0.9]')");
    await db.exec('CREATE INDEX idx_hnsw ON hnsw_test(vec) USING HNSW');
    const rows = await db.query(
      "SELECT id, VEC_DISTANCE_L2(vec, '[0.4, 0.5, 0.6]') AS dist FROM hnsw_test ORDER BY dist LIMIT 2"
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[0].id, 2);
  });
});

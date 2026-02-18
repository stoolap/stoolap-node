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

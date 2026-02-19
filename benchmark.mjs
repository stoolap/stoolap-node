// Stoolap vs SQLite (better-sqlite3) Node.js benchmark
// Both drivers use synchronous methods for fair comparison.
// Both use autocommit — each write is an implicit transaction + commit.
// Matches examples/benchmark.rs test set and ordering.
//
// Run:  node benchmark.mjs

import { performance } from 'node:perf_hooks';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { Database: StoolapDB } = require('./index.js');
const BetterSqlite3 = require('better-sqlite3');

const ROW_COUNT = 10_000;
const ITERATIONS = 500;       // Point queries
const ITERATIONS_MEDIUM = 250; // Index scans, aggregations
const ITERATIONS_HEAVY = 50;   // Full scans, JOINs
const WARMUP = 10;

// ============================================================
// Helpers
// ============================================================

function fmtUs(us) {
  return us.toFixed(3).padStart(15);
}

function fmtRatio(stoolapUs, sqliteUs) {
  if (stoolapUs <= 0 || sqliteUs <= 0) return '      -';
  const ratio = sqliteUs / stoolapUs;
  if (ratio >= 1) {
    return `${ratio.toFixed(2)}x`.padStart(10);
  } else {
    return `${(1 / ratio).toFixed(2)}x`.padStart(9) + '*';
  }
}

let stoolapWins = 0;
let sqliteWins = 0;

function printRow(name, stoolapUs, sqliteUs) {
  const ratio = fmtRatio(stoolapUs, sqliteUs);
  if (stoolapUs < sqliteUs) stoolapWins++;
  else if (sqliteUs < stoolapUs) sqliteWins++;
  console.log(
    `${name.padEnd(28)} | ${fmtUs(stoolapUs)} | ${fmtUs(sqliteUs)} | ${ratio}`
  );
}

function printHeader(section) {
  console.log('');
  console.log(`${'='.repeat(80)}`);
  console.log(section);
  console.log(`${'='.repeat(80)}`);
  console.log(
    `${'Operation'.padEnd(28)} | ${'Stoolap (μs)'.padStart(15)} | ${'SQLite (μs)'.padStart(15)} | ${'Ratio'.padStart(10)}`
  );
  console.log('-'.repeat(80));
}

function seedRandom(i) {
  return ((i * 1103515245 + 12345) & 0x7fffffff);
}

// ============================================================
// Setup databases
// ============================================================

async function main() {
  console.log('Stoolap vs SQLite (better-sqlite3) — Node.js Benchmark');
  console.log(`Configuration: ${ROW_COUNT} rows, ${ITERATIONS} iterations per test`);
  console.log('All operations are synchronous — fair comparison');
  console.log('Ratio > 1x = Stoolap faster  |  * = SQLite faster\n');

  // --- Stoolap setup ---
  const sdb = await StoolapDB.open(':memory:');
  sdb.execSync(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance FLOAT NOT NULL,
      active BOOLEAN NOT NULL,
      created_at TEXT NOT NULL
    )
  `);
  sdb.execSync('CREATE INDEX idx_users_age ON users(age)');
  sdb.execSync('CREATE INDEX idx_users_active ON users(active)');

  // --- SQLite setup ---
  const ldb = new BetterSqlite3(':memory:');
  ldb.pragma('journal_mode = WAL');
  ldb.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance REAL NOT NULL,
      active INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
  `);
  ldb.exec('CREATE INDEX idx_users_age ON users(age)');
  ldb.exec('CREATE INDEX idx_users_active ON users(active)');

  // --- Populate users ---
  const sInsert = sdb.prepare(
    'INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)'
  );
  const lInsert = ldb.prepare(
    'INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
  );
  const lInsertMany = ldb.transaction((rows) => {
    for (const r of rows) lInsert.run(...r);
  });

  const userRows = [];
  for (let i = 1; i <= ROW_COUNT; i++) {
    const age = (seedRandom(i) % 62) + 18;
    const balance = (seedRandom(i * 7) % 100000) + (seedRandom(i * 13) % 100) / 100;
    const active = seedRandom(i * 3) % 10 < 7 ? 1 : 0;
    const name = `User_${i}`;
    const email = `user${i}@example.com`;
    userRows.push([i, name, email, age, balance, active, '2024-01-01 00:00:00']);
  }

  lInsertMany(userRows);
  sInsert.executeBatchSync(userRows);

  // ============================================================
  // CORE OPERATIONS (matches benchmark.rs section 1)
  // ============================================================
  printHeader('CORE OPERATIONS');

  // --- SELECT by ID ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE id = $1');
    const lSt = ldb.prepare('SELECT * FROM users WHERE id = ?');
    const ids = Array.from({ length: ITERATIONS }, (_, i) => (i % ROW_COUNT) + 1);

    for (let i = 0; i < WARMUP; i++) { sSt.queryOneSync([ids[i]]); lSt.get(ids[i]); }

    let t = performance.now();
    for (const id of ids) sSt.queryOneSync([id]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (const id of ids) lSt.get(id);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('SELECT by ID', sUs, lUs);
  }

  // --- SELECT by index (exact) ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE age = $1');
    const lSt = ldb.prepare('SELECT * FROM users WHERE age = ?');
    const ages = Array.from({ length: ITERATIONS }, (_, i) => (i % 62) + 18);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync([ages[i]]); lSt.all(ages[i]); }

    let t = performance.now();
    for (const age of ages) sSt.querySync([age]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (const age of ages) lSt.all(age);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('SELECT by index (exact)', sUs, lUs);
  }

  // --- SELECT by index (range) ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE age >= $1 AND age <= $2');
    const lSt = ldb.prepare('SELECT * FROM users WHERE age >= ? AND age <= ?');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync([30, 40]); lSt.all(30, 40); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync([30, 40]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all(30, 40);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('SELECT by index (range)', sUs, lUs);
  }

  // --- SELECT complex ---
  {
    const sSt = sdb.prepare(
      'SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100'
    );

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('SELECT complex', sUs, lUs);
  }

  // --- SELECT * (full scan) ---
  {
    const sSt = sdb.prepare('SELECT * FROM users');
    const lSt = ldb.prepare('SELECT * FROM users');

    for (let i = 0; i < WARMUP; i++) { sSt.queryRawSync(); lSt.raw(true).all(); lSt.raw(false); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS_HEAVY; i++) sSt.queryRawSync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS_HEAVY;

    lSt.raw(true);
    t = performance.now();
    for (let i = 0; i < ITERATIONS_HEAVY; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS_HEAVY;
    lSt.raw(false);

    printRow('SELECT * (full scan)', sUs, lUs);
  }

  // --- UPDATE by ID ---
  {
    const sSt = sdb.prepare('UPDATE users SET balance = $1 WHERE id = $2');
    const lSt = ldb.prepare('UPDATE users SET balance = ? WHERE id = ?');
    const params = Array.from({ length: ITERATIONS }, (_, i) => [
      (seedRandom(i * 17) % 100000) + 0.5,
      (i % ROW_COUNT) + 1,
    ]);

    for (let i = 0; i < WARMUP; i++) { sSt.executeSync(params[i]); lSt.run(...params[i]); }

    let t = performance.now();
    for (const p of params) sSt.executeSync(p);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (const p of params) lSt.run(...p);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('UPDATE by ID', sUs, lUs);
  }

  // --- UPDATE complex ---
  {
    const sSt = sdb.prepare('UPDATE users SET balance = $1 WHERE age >= $2 AND age <= $3 AND active = true');
    const lSt = ldb.prepare('UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = 1');
    const balances = Array.from({ length: ITERATIONS }, (_, i) =>
      (seedRandom(i * 23) % 100000) + 0.5
    );

    for (let i = 0; i < WARMUP; i++) { sSt.executeSync([balances[i], 27, 28]); lSt.run(balances[i], 27, 28); }

    let t = performance.now();
    for (const bal of balances) sSt.executeSync([bal, 27, 28]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (const bal of balances) lSt.run(bal, 27, 28);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('UPDATE complex', sUs, lUs);
  }

  // --- INSERT single ---
  {
    const sSt = sdb.prepare(
      'INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)'
    );
    const lSt = ldb.prepare(
      'INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
    );
    const base = ROW_COUNT + 1000;

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
      const id = base + i;
      sSt.executeSync([id, `New_${id}`, `new${id}@example.com`, (seedRandom(i * 29) % 62) + 18, 100.0, 1, '2024-01-01 00:00:00']);
    }
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
      const id = base + ITERATIONS + i;
      lSt.run(id, `New_${id}`, `new${id}@example.com`, (seedRandom(i * 29) % 62) + 18, 100.0, 1, '2024-01-01 00:00:00');
    }
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('INSERT single', sUs, lUs);
  }

  // --- DELETE by ID ---
  {
    const sSt = sdb.prepare('DELETE FROM users WHERE id = $1');
    const lSt = ldb.prepare('DELETE FROM users WHERE id = ?');
    const base = ROW_COUNT + 1000;

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.executeSync([base + i]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.run(base + ITERATIONS + i);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('DELETE by ID', sUs, lUs);
  }

  // --- DELETE complex ---
  {
    const sSt = sdb.prepare('DELETE FROM users WHERE age >= $1 AND age <= $2 AND active = true');
    const lSt = ldb.prepare('DELETE FROM users WHERE age >= ? AND age <= ? AND active = 1');

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.executeSync([25, 26]);
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.run(25, 26);
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('DELETE complex', sUs, lUs);
  }

  // --- Aggregation (GROUP BY) ---
  {
    const sSt = sdb.prepare('SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age');
    const lSt = ldb.prepare('SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS_MEDIUM; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS_MEDIUM;

    t = performance.now();
    for (let i = 0; i < ITERATIONS_MEDIUM; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS_MEDIUM;

    printRow('Aggregation (GROUP BY)', sUs, lUs);
  }

  // ============================================================
  // ADVANCED OPERATIONS (matches benchmark.rs section 2)
  // ============================================================

  // Create orders table
  sdb.execSync(`
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount FLOAT NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
  `);
  sdb.execSync('CREATE INDEX idx_orders_user_id ON orders(user_id)');
  sdb.execSync('CREATE INDEX idx_orders_status ON orders(status)');

  ldb.exec(`
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
  `);
  ldb.exec('CREATE INDEX idx_orders_user_id ON orders(user_id)');
  ldb.exec('CREATE INDEX idx_orders_status ON orders(status)');

  // Populate orders (3 per user on average)
  const sOrderInsert = sdb.prepare(
    'INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)'
  );
  const lOrderInsert = ldb.prepare(
    'INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)'
  );
  const statuses = ['pending', 'completed', 'shipped', 'cancelled'];
  const orderRows = [];
  for (let i = 1; i <= ROW_COUNT * 3; i++) {
    const userId = (seedRandom(i * 11) % ROW_COUNT) + 1;
    const amount = (seedRandom(i * 19) % 990) + 10 + (seedRandom(i * 23) % 100) / 100;
    const status = statuses[seedRandom(i * 31) % 4];
    orderRows.push([i, userId, amount, status, '2024-01-15']);
  }
  const lOrderMany = ldb.transaction((rows) => { for (const r of rows) lOrderInsert.run(...r); });
  lOrderMany(orderRows);
  sOrderInsert.executeBatchSync(orderRows);

  printHeader('ADVANCED OPERATIONS');

  // --- INNER JOIN ---
  {
    const sSt = sdb.prepare(
      "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100"
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('INNER JOIN', sUs, lUs);
  }

  // --- LEFT JOIN + GROUP BY ---
  {
    const sSt = sdb.prepare(
      'SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100'
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('LEFT JOIN + GROUP BY', sUs, lUs);
  }

  // --- Scalar subquery ---
  {
    const sql = 'SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Scalar subquery', sUs, lUs);
  }

  // --- IN subquery ---
  {
    const sSt = sdb.prepare(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100"
    );
    const iters = 10;

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('IN subquery', sUs, lUs);
  }

  // --- EXISTS subquery ---
  {
    const sSt = sdb.prepare(
      'SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100'
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('EXISTS subquery', sUs, lUs);
  }

  // --- CTE + JOIN ---
  {
    const sSt = sdb.prepare(
      'WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100'
    );
    const lSt = ldb.prepare(
      'WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100'
    );
    const iters = 20;

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('CTE + JOIN', sUs, lUs);
  }

  // --- Window ROW_NUMBER ---
  {
    const sql = 'SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Window ROW_NUMBER', sUs, lUs);
  }

  // --- Window ROW_NUMBER (PK) ---
  {
    const sql = 'SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Window ROW_NUMBER (PK)', sUs, lUs);
  }

  // --- Window PARTITION BY ---
  {
    const sql = 'SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Window PARTITION BY', sUs, lUs);
  }

  // --- UNION ALL ---
  {
    const sSt = sdb.prepare(
      "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100"
    );

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('UNION ALL', sUs, lUs);
  }

  // --- CASE expression ---
  {
    const sSt = sdb.prepare(
      "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100"
    );

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('CASE expression', sUs, lUs);
  }

  // --- Complex JOIN+GROUP+HAVING ---
  {
    const sSt = sdb.prepare(
      "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50"
    );
    const lSt = ldb.prepare(
      "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = 1 AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50"
    );
    const iters = 20;

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Complex JOIN+GRP+HAVING', sUs, lUs);
  }

  // --- Batch INSERT (100 rows in transaction) ---
  {
    const iters = ITERATIONS;
    const baseId = ROW_COUNT * 10;
    const insertSql = 'INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)';
    const lInsertSt = ldb.prepare('INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)');

    let t = performance.now();
    for (let iter = 0; iter < iters; iter++) {
      const batch = [];
      for (let j = 0; j < 100; j++) {
        const id = baseId + iter * 100 + j;
        batch.push([id, 1, 100.0, 'pending', '2024-02-01']);
      }
      sdb.executeBatchSync(insertSql, batch);
    }
    const sUs = ((performance.now() - t) * 1000) / iters;

    const lBatchInsert = ldb.transaction((batch) => {
      for (const r of batch) lInsertSt.run(...r);
    });

    t = performance.now();
    for (let iter = 0; iter < iters; iter++) {
      const batch = [];
      for (let j = 0; j < 100; j++) {
        const id = baseId + iters * 100 + iter * 100 + j;
        batch.push([id, 1, 100.0, 'pending', '2024-02-01']);
      }
      lBatchInsert(batch);
    }
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Batch INSERT (100 rows)', sUs, lUs);
  }

  // ============================================================
  // BOTTLENECK HUNTERS (matches benchmark.rs section 3)
  // ============================================================
  printHeader('BOTTLENECK HUNTERS');

  // --- DISTINCT (no ORDER) ---
  {
    const sSt = sdb.prepare('SELECT DISTINCT age FROM users');
    const lSt = ldb.prepare('SELECT DISTINCT age FROM users');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('DISTINCT (no ORDER)', sUs, lUs);
  }

  // --- DISTINCT + ORDER BY ---
  {
    const sSt = sdb.prepare('SELECT DISTINCT age FROM users ORDER BY age');
    const lSt = ldb.prepare('SELECT DISTINCT age FROM users ORDER BY age');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('DISTINCT + ORDER BY', sUs, lUs);
  }

  // --- COUNT DISTINCT ---
  {
    const sSt = sdb.prepare('SELECT COUNT(DISTINCT age) FROM users');
    const lSt = ldb.prepare('SELECT COUNT(DISTINCT age) FROM users');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('COUNT DISTINCT', sUs, lUs);
  }

  // --- LIKE prefix ---
  {
    const sSt = sdb.prepare("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100");
    const lSt = ldb.prepare("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100");

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('LIKE prefix (User_1%)', sUs, lUs);
  }

  // --- LIKE contains ---
  {
    const sSt = sdb.prepare("SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100");
    const lSt = ldb.prepare("SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100");

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('LIKE contains (%50%)', sUs, lUs);
  }

  // --- OR conditions ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100');
    const lSt = ldb.prepare('SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('OR conditions (3 vals)', sUs, lUs);
  }

  // --- IN list ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100');
    const lSt = ldb.prepare('SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('IN list (7 values)', sUs, lUs);
  }

  // --- NOT IN subquery ---
  {
    const sSt = sdb.prepare(
      "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100"
    );
    const iters = 10;

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('NOT IN subquery', sUs, lUs);
  }

  // --- NOT EXISTS subquery ---
  {
    const sSt = sdb.prepare(
      "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100"
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('NOT EXISTS subquery', sUs, lUs);
  }

  // --- OFFSET pagination ---
  {
    const sSt = sdb.prepare('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000');
    const lSt = ldb.prepare('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('OFFSET pagination (5000)', sUs, lUs);
  }

  // --- Multi-column ORDER BY ---
  {
    const sSt = sdb.prepare('SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100');
    const lSt = ldb.prepare('SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Multi-col ORDER BY (3)', sUs, lUs);
  }

  // --- Self JOIN (same age) ---
  {
    const sSt = sdb.prepare(
      'SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100'
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Self JOIN (same age)', sUs, lUs);
  }

  // --- Multi window funcs (3) ---
  {
    const sql = 'SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Multi window funcs (3)', sUs, lUs);
  }

  // --- Nested subquery (3 levels) ---
  {
    const sql = 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);
    const iters = 20;

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Nested subquery (3 lvl)', sUs, lUs);
  }

  // --- Multi aggregates (6) ---
  {
    const sql = 'SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Multi aggregates (6)', sUs, lUs);
  }

  // --- COALESCE + IS NOT NULL ---
  {
    const sql = 'SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('COALESCE + IS NOT NULL', sUs, lUs);
  }

  // --- Expr in WHERE (funcs) ---
  {
    const sSt = sdb.prepare(
      "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100"
    );
    const lSt = ldb.prepare(
      "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100"
    );

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Expr in WHERE (funcs)', sUs, lUs);
  }

  // --- Math expressions ---
  {
    const sql = 'SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Math expressions', sUs, lUs);
  }

  // --- String concat (||) ---
  {
    const sql = "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100";
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('String concat (||)', sUs, lUs);
  }

  // --- Large result (no LIMIT) ---
  {
    const sSt = sdb.prepare('SELECT id, name, balance FROM users WHERE active = true');
    const lSt = ldb.prepare('SELECT id, name, balance FROM users WHERE active = 1');
    const iters = 20;

    for (let i = 0; i < 5; i++) { sSt.queryRawSync(); lSt.raw(true).all(); lSt.raw(false); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.queryRawSync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    lSt.raw(true);
    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;
    lSt.raw(false);

    printRow('Large result (no LIMIT)', sUs, lUs);
  }

  // --- Multiple CTEs (2) ---
  {
    const sSt = sdb.prepare(
      'WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50'
    );
    const lSt = ldb.prepare(
      'WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50'
    );
    const iters = 100;
    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Multiple CTEs (2)', sUs, lUs);
  }

  // --- Correlated in SELECT ---
  {
    const sSt = sdb.prepare(
      'SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100'
    );
    const iters = 100;
    for (let i = 0; i < 5; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < iters; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / iters;

    t = performance.now();
    for (let i = 0; i < iters; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / iters;

    printRow('Correlated in SELECT', sUs, lUs);
  }

  // --- BETWEEN (non-indexed) ---
  {
    const sSt = sdb.prepare('SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100');
    const lSt = ldb.prepare('SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('BETWEEN (non-indexed)', sUs, lUs);
  }

  // --- GROUP BY (2 columns) ---
  {
    const sSt = sdb.prepare('SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active');
    const lSt = ldb.prepare('SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active');

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('GROUP BY (2 columns)', sUs, lUs);
  }

  // --- CROSS JOIN (limited) ---
  {
    const sSt = sdb.prepare(
      'SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100'
    );
    const lSt = ldb.prepare(
      'SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100'
    );

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('CROSS JOIN (limited)', sUs, lUs);
  }

  // --- Derived table (FROM subquery) ---
  {
    const sql = "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group";
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Derived table (FROM sub)', sUs, lUs);
  }

  // --- Window ROWS frame ---
  {
    const sql = 'SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Window ROWS frame', sUs, lUs);
  }

  // --- HAVING complex ---
  {
    const sql = 'SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('HAVING complex', sUs, lUs);
  }

  // --- Compare with subquery ---
  {
    const sql = 'SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100';
    const sSt = sdb.prepare(sql);
    const lSt = ldb.prepare(sql);

    for (let i = 0; i < WARMUP; i++) { sSt.querySync(); lSt.all(); }

    let t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) sSt.querySync();
    const sUs = ((performance.now() - t) * 1000) / ITERATIONS;

    t = performance.now();
    for (let i = 0; i < ITERATIONS; i++) lSt.all();
    const lUs = ((performance.now() - t) * 1000) / ITERATIONS;

    printRow('Compare with subquery', sUs, lUs);
  }

  // ============================================================
  // Summary
  // ============================================================
  console.log('\n' + '='.repeat(80));
  console.log(`SCORE: Stoolap ${stoolapWins} wins  |  SQLite ${sqliteWins} wins`);
  console.log('');
  console.log('NOTES:');
  console.log('- Both drivers use synchronous methods — fair comparison');
  console.log('- Both use autocommit (each write = implicit transaction + commit)');
  console.log('- Ratio > 1x = Stoolap faster  |  * = SQLite faster');
  console.log('='.repeat(80));

  await sdb.close();
  ldb.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

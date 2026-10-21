export interface RunResult {
  changes: number;
}

export declare class Database {
  /**
   * Open a database asynchronously. Returns a Promise that resolves to a Database instance.
   *
   * Accepts:
   * - `:memory:` or empty string for in-memory database
   * - `memory://` for in-memory database
   * - `file:///path/to/db` for file-based database
   * - Bare path like `./mydb` for file-based database
   */
  static open(path: string): Promise<Database>;
  /**
   * Open a database synchronously.
   */
  static openSync(path: string): Database;
  /**
   * Clone this database handle. The clone shares the same underlying engine
   * (data, indexes, transactions) but has its own executor and error state.
   * Each clone must be closed independently.
   */
  clone(): Database;
  /**
   * Execute a DDL/DML statement. Returns Promise<{ changes: number }>.
   *
   * @param sql - SQL statement
   * @param params - Optional: Array for positional ($1, $2) or Object for named (:key)
   */
  execute(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<RunResult>;
  /**
   * Execute a DDL statement. Returns Promise<void>.
   */
  exec(sql: string): Promise<void>;
  /**
   * Query rows. Returns Promise<Array<Object>>.
   *
   * Each row is an object with column names as keys.
   */
  query(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<Record<string, any>[]>;
  /** Query a single row. Returns Promise<Object | null>. */
  queryOne(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<Record<string, any> | null>;
  /**
   * Query rows in raw format. Returns Promise<{ columns: string[], rows: any[][] }>.
   *
   * Faster than query(). Skips per-row object creation.
   */
  queryRaw(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<{ columns: string[]; rows: any[][] }>;
  /**
   * Execute a DML statement synchronously. Returns { changes: number }.
   *
   * Faster than execute() for simple operations. No async overhead.
   */
  executeSync(sql: string, params?: any[] | Record<string, any>): RunResult;
  /** Execute a DDL statement synchronously. */
  execSync(sql: string): void;
  /** Query rows synchronously. Returns Array<Object>. */
  querySync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Record<string, any>[];
  /** Query a single row synchronously. Returns Object | null. */
  queryOneSync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Record<string, any> | null;
  /** Query rows in raw format synchronously. Returns { columns: string[], rows: any[][] }. */
  queryRawSync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): { columns: string[]; rows: any[][] };
  /**
   * Execute the same SQL with multiple param sets in a single call.
   * Auto-wraps in a transaction: begin, execute all, commit.
   * Returns { changes: total_rows_affected }.
   */
  executeBatchSync(
    sql: string,
    paramsArray: any[][] | Record<string, any>[],
  ): RunResult;
  /** Create a prepared statement (synchronous). */
  prepare(sql: string): PreparedStatement;
  /** Begin a transaction. Returns Promise<Transaction>. */
  begin(): Promise<Transaction>;
  /** Begin a transaction synchronously. Returns Transaction. */
  beginSync(): Transaction;
  /** Close the database. Returns Promise<void>. */
  close(): Promise<void>;
  /**
   * Open a cursor over the query's rows. Iterate it with `for...of` or
   * `for await...of`; rows are fetched in chunks as the loop advances and
   * the cursor closes itself when the loop ends, breaks or throws.
   */
  iterate(sql: string, params?: any[] | Record<string, any>): RowCursor;
  /** Close the database synchronously. */
  closeSync(): void;
}
export type JsDatabase = Database;

/**
 * A cursor over an open result set, from `iterate()`. Iterable both
 * synchronously and with `for await`. Rows cross the native boundary in
 * chunks of 32 to 256 rows.
 */
export declare class RowCursor
  implements Iterable<Record<string, any>>, AsyncIterable<Record<string, any>>
{
  [Symbol.iterator](): Iterator<Record<string, any>>;
  [Symbol.asyncIterator](): AsyncIterator<Record<string, any>>;
  /** End the iteration early and release the result set. */
  close(): void;
  /** True once the cursor was closed or its result set was exhausted. */
  get closed(): boolean;
}

export declare class PreparedStatement {
  /** Execute the statement (DML). Returns Promise<{ changes: number }>. */
  execute(params?: any[] | Record<string, any>): Promise<RunResult>;
  /** Query rows. Returns Promise<Array<Object>>. */
  query(params?: any[] | Record<string, any>): Promise<Record<string, any>[]>;
  /** Query single row. Returns Promise<Object | null>. */
  queryOne(
    params?: any[] | Record<string, any>,
  ): Promise<Record<string, any> | null>;
  /** Query rows in raw format. Returns Promise<{ columns: string[], rows: any[][] }>. */
  queryRaw(
    params?: any[] | Record<string, any>,
  ): Promise<{ columns: string[]; rows: any[][] }>;
  /** Execute synchronously. Returns { changes: number }. */
  executeSync(params?: any[] | Record<string, any>): RunResult;
  /** Query rows synchronously. Returns Array<Object>. */
  querySync(params?: any[] | Record<string, any>): Record<string, any>[];
  /** Query single row synchronously. Returns Object | null. */
  queryOneSync(
    params?: any[] | Record<string, any>,
  ): Record<string, any> | null;
  /** Query rows in raw format synchronously. Returns { columns: string[], rows: any[][] }. */
  queryRawSync(params?: any[] | Record<string, any>): {
    columns: string[];
    rows: any[][];
  };
  /**
   * Execute the prepared SQL with multiple param sets in a single call.
   * Returns { changes: total_rows_affected }.
   */
  executeBatchSync(paramsArray: any[][] | Record<string, any>[]): RunResult;
  /** Open a cursor over the statement's rows; see `Database.iterate`. */
  iterate(params?: any[] | Record<string, any>): RowCursor;
  /** Get the SQL text of this prepared statement. */
  get sql(): string;
  /** Release the prepared statement. */
  finalize(): void;
}
export type JsPreparedStatement = PreparedStatement;

export declare class Transaction {
  /** Execute a DML statement within the transaction. Returns Promise<{ changes: number }>. */
  execute(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<RunResult>;
  /** Query rows within the transaction. Returns Promise<Array<Object>>. */
  query(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<Record<string, any>[]>;
  /** Query a single row within the transaction. Returns Promise<Object | null>. */
  queryOne(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<Record<string, any> | null>;
  /** Query rows in raw format within the transaction. Returns Promise<{ columns: string[], rows: any[][] }>. */
  queryRaw(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Promise<{ columns: string[]; rows: any[][] }>;
  /** Commit the transaction. Returns Promise<void>. */
  commit(): Promise<void>;
  /** Rollback the transaction. Returns Promise<void>. */
  rollback(): Promise<void>;
  /** Execute a DML statement synchronously. Returns { changes: number }. */
  executeSync(sql: string, params?: any[] | Record<string, any>): RunResult;
  /** Query rows synchronously. Returns Array<Object>. */
  querySync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Record<string, any>[];
  /** Query a single row synchronously. Returns Object | null. */
  queryOneSync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): Record<string, any> | null;
  /** Query rows in raw format synchronously. Returns { columns: string[], rows: any[][] }. */
  queryRawSync(
    sql: string,
    params?: any[] | Record<string, any>,
  ): { columns: string[]; rows: any[][] };
  /** Commit the transaction synchronously. */
  commitSync(): void;
  /**
   * Execute the same SQL with multiple param sets in a single call.
   * Returns { changes: total_rows_affected }.
   */
  executeBatchSync(
    sql: string,
    paramsArray: any[][] | Record<string, any>[],
  ): RunResult;
  /**
   * Open a cursor over the query's rows inside the transaction; see
   * `Database.iterate`. Commit and rollback close open cursors.
   */
  iterate(sql: string, params?: any[] | Record<string, any>): RowCursor;
  /** Rollback the transaction synchronously. */
  rollbackSync(): void;
}
export type JsTransaction = Transaction;

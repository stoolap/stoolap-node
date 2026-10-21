"use strict";

const { Database } = require("./lib/database");
const { Transaction } = require("./lib/transaction");
const { PreparedStatement } = require("./lib/statement");
const { RowCursor } = require("./lib/iterator");

module.exports = { Database, Transaction, PreparedStatement, RowCursor };
module.exports.Database = Database;
module.exports.PreparedStatement = PreparedStatement;
module.exports.Transaction = Transaction;
module.exports.RowCursor = RowCursor;
// Legacy aliases
module.exports.JsDatabase = Database;
module.exports.JsPreparedStatement = PreparedStatement;
module.exports.JsTransaction = Transaction;

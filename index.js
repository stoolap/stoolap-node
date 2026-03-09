"use strict";

const { Database } = require("./lib/database");
const { Transaction } = require("./lib/transaction");
const { PreparedStatement } = require("./lib/statement");

module.exports = { Database, Transaction, PreparedStatement };
module.exports.Database = Database;
module.exports.PreparedStatement = PreparedStatement;
module.exports.Transaction = Transaction;
// Legacy aliases
module.exports.JsDatabase = Database;
module.exports.JsPreparedStatement = PreparedStatement;
module.exports.JsTransaction = Transaction;

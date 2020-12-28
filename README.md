# PHP MySQL Engine

This is a PHP port of Slack's [Hack SQL Fake](https://github.com/slackhq/hack-sql-fake) created by [Scott Sandler](https://github.com/ssandler).

PHP MySQL Engine is a unit testing library for PHP. It enables testing database-driven applications with an in-memory simulation of MySQL. It supports a wide variety of queries, transactions, and more. This project extends the `PDO` class and allows you to call common PDO MySQL methods.

## Motivation

In most unit testing libraries, SQL queries are traditionally replaced with Mock or Stub implementations. Mocks require an explicit list of queries that are expected to run and results to return, while stubs may not even check the queries being run and simply return a hard coded result. This leads to significant manual work setting up expectations, and tests which are fragile and must be updated even on benign changes to the code or queries. It also means the data access layer is not unit tested.

Another common strategy is to test using an actual database, such as SQLite. This creates a situation in which the database in tests may not match the behavior of the production database, and any code using specialized features of the production database may be untestable. It also means that different test cases are not isolated from each other, which can make tests difficult to debug. This can be resolved by truncating tables between each test case, but that can create a performance problem.

PHP MySQL Engine takes a different approach - it parses and executes `SELECT`, `INSERT`, `UPDATE`, and `DELETE` queries against an in-memory "database" stored in PHP arrays. As long as the amount of data used for testing is small, this solves the problems mentioned above.

## SQL Syntax Supported

This library supports a wide variety of query syntax, including:

- `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` clauses supported as appropriate for each query type
- `JOIN` queries with all join types
- multi-queries such as subqueries, `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- complex expressions such as `CASE`, `BETWEEN`, and row comparators `(1, 2, 3) < (4, 5, 6)`
- all basic operators implemented with operator precedence
- column aliases, cross-database queries
- `INSERT ... ON DUPLICATE KEY UPDATE`
- A variety of SQL functions such as `COUNT(), NULLIF(), COALESCE(), CONCAT_WS()` and many others
- Strict SQL mode can be enabled or disabled to throw exceptions for invalid data types and missing not-null fields
- Validating parser: the query parser will throw exceptions on most invalid SQL Queries, helping protect your production environment from accidental SQL syntax errors

For an overview of everything that's supported, see the `tests/` for this library, which document nearly all of the SQL features it supports.

## Usage

PHP MySQL Engine works by providing a subclass of [PDO](https://www.php.net/manual/en/class.pdo.php).

This library assumes you currently have some form of establishing a database connection using `PDO`. The best way to use PHP MySQL Engine will depend on your code, but you can use dependency injection to instantiate a `Vimeo\MysqlEngine\FakePdo` object when testing. This will behave like a database for the rest of your test run.

The rest of your code can operate as normal, using the database in the same way it is used in production.

## Why doesn't it support `X`?

This library aims to support everything its users use in MySQL, rather than every possibly feature MySQL offers. We welcome pull requests to add support for new syntax, sql functions, data types, bug fixes, and other features. See our #issues page for a wishlist.

## Contributing

See [Contributing](CONTRIBUTING.md)

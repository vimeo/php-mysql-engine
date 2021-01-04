# PHP MySQL Engine

This is a PHP port of Slack's [Hack SQL Fake](https://github.com/slackhq/hack-sql-fake) created by [Scott Sandler](https://github.com/ssandler).

PHP MySQL Engine is a unit testing library for PHP. It enables testing database-driven applications with an in-memory simulation of MySQL. This project extends the `PDO` class and allows you to call common PDO MySQL methods. It supports a wide variety of queries, and some PDO-specific functionality like transactions and different fetch modes.

## Motivation

Currently there are two ways to test code that reads and writes to a database:

- Mock SQL query execution<br/>
  Mocks require an explicit list of queries that are expected to run and results to return. This leads to significant manual work setting up expectations, and tests which are fragile and must be updated even on benign changes to the code or queries. It also means the data access layer is not unit tested.
  
- Use an actual database<br />
  It might make sense to test with a separate database instance – this is we have done at Vimeo. But databases like MySQL are designed to be filled with lots of long-lasting data, whereas unit tests write small amounts of very short-lived data. This means that extra care has to be taken to make sure that test databases are truncated between tests, which creates a performance issue.

PHP MySQL Engine takes a different approach - it parses and executes `SELECT`, `INSERT`, `UPDATE`, and `DELETE` queries against an in-memory "database" stored in PHP arrays. As long as the amount of data used for testing is small, this solves the problems mentioned above.

## Caveat Emptor

Unlike [Psalm](https://github.com/vimeo/psalm), this package is not designed with a wide audience in mind. For a project to really benefit from this library it should already have a large number of unit tests that require a database connection to complete, and the project maintainers must understand the tradeoffs associated with using an unofficial MySQL implementation in their test suite.

Pull requests are welcome, but this project doesn’t have an issue tracker as it won’t be actively maintained. If you want to fork the project, feel free!

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

This library aims to support everything its users use in MySQL, rather than every possibly feature MySQL offers. We welcome pull requests to add support for new syntax, sql functions, data types, bug fixes, and other features.

### Currently unsupported

- MySQL temporary variables (these will likely never be supported)
- MySQL date functions

## Contributing

See [Contributing](CONTRIBUTING.md)

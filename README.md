# PHP MySQL Engine

PHP MySQL Engine is a library for PHP that allows you to test database-driven applications with an in-memory simulation of MySQL 5.6. This project extends the `PDO` class and allows you to call common PDO MySQL methods. It supports a wide variety of queries, and some PDO-specific functionality like transactions and different fetch modes.

PHP MySQL Engine is based on Slack's [Hack SQL Fake](https://github.com/slackhq/hack-sql-fake) created by [Scott Sandler](https://github.com/ssandler).

You can read an article about this tool [here](https://medium.com/vimeo-engineering-blog/the-great-pretender-faster-application-tests-with-mysql-simulation-26250f13d251).

## Motivation

Currently there are two ways to test code that reads and writes to a database:

- Mock SQL query execution<br/>
  Mocks require an explicit list of queries that are expected to run and results to return. This leads to significant manual work setting up expectations, and tests which are fragile and must be updated even on benign changes to the code or queries. It also means the data access layer is not unit tested.
  
- Use an actual database<br />
  It might make sense to test with a separate database instance – this is what we have done in the past at Vimeo. But databases like MySQL are designed to be filled with lots of long-lasting data, whereas unit tests write small amounts of very short-lived data. This means that extra care has to be taken to make sure that test databases are truncated between tests, which creates a performance issue.

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
- Temporary variables like `@previous_name := user.name`
- Validating parser: the query parser will throw exceptions on most invalid SQL Queries, helping protect your production environment from accidental SQL syntax errors

## Unsupported MySQL features

This engine does _not_ support [MySQL Stored objects](https://dev.mysql.com/doc/refman/5.6/en/stored-objects.html), which precludes the testing of stored procedures, triggers and views.

## Caveat Emptor

Unlike [Psalm](https://github.com/vimeo/psalm), this package is not designed with a wide audience in mind. For a project to really benefit from this library it should already have a large number of tests that require a database connection to complete, and the project maintainers must understand the tradeoffs associated with using an unofficial MySQL implementation in their test suite.

## Known issues

### Result types when not emulating prepares

By default the engine returns all data formatted as a string. If `$pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false)` is called, the engine will instead infer column types (for example, `SUM(some_int_column)` will be given an `int` type). In some cases `php-mysql-engine` may do a better job of inferring correct column types than actual MySQL, which defaults to string when it can’t work out a column type. If you do strict type checks on the results you may see small discrepancies.

## Installation

```
composer require --dev vimeo/php-mysql-engine
```

## Usage

PHP MySQL Engine works by providing a subclass of [PDO](https://www.php.net/manual/en/class.pdo.php).

You can instantiate the subclass as you would `PDO`, and use dependency injection or similar to provide that instance to your application code.

```php
// use a class specific to your current PHP version (APIs changed in major versions)
$pdo = new \Vimeo\MysqlEngine\Php8\FakePdo($dsn, $user, $password);
// currently supported attributes
$pdo->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
$pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
```

The rest of your code can operate as normal, using the database in the same way it is used in production.

## Why doesn't it support X?

This library aims to support everything its users use in MySQL, rather than every possibly feature MySQL offers. We welcome pull requests to add support for new syntax, sql functions, data types, bug fixes, and other features.

## Why doesn’t this project have an issue tracker?

Maintaining open-source projects is hard work, and I don't want to make more work for me or my colleagues. Use this project very much use at your own risk.

If you want to fork the project with an issue tracker, feel free!

## Contributing

If you want to create a PR, please make sure it passes unit tests:

```
vendor/bin/phpunit
```

and also Psalm's checks

```
vendor/bin/psalm
```

Thanks!

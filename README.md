# Hack SQL Fake

[![Build Status](https://travis-ci.org/slackhq/hack-sql-fake.svg?branch=master)](https://travis-ci.org/slackhq/hack-sql-fake)

Hack SQL Fake is unit testing library for [Hack](https://hacklang.org/). It enables testing database-driven applications with an in-memory simulation of MySQL. It supports a wide variety of queries, rapid snapshot/restore of the database between test cases, and more. This is done with a [Fake Object](https://martinfowler.com/bliki/TestDouble.html, which contains an implementation of the database, avoiding the need for explicit stubbing or mocking.

## Motivation

In most unit testing libraries, SQL database calls are traditionally replaced with Mock or Stub implementations. Mocks require an explicit list of queries that are expected to run and hard code the results that will be returned, while stubs may not even check the queries being run and simply return the hard coded result. This leads to a lot of manual work setting up expectations, and tests which are fragile and must be updated even on benign changes to the code or queries. It also means the database interaction library is not unit tested.

Another common strategy is to test using an actual database, such as SQLite. This creates a dangerous situation in which the query engine in tests may not match the behavior of the production database, and any code using specialized features of the production database may be untestable. This strategy also requires explicitly truncating the schema and reloading test data between test cases to ensure isolation.

Hack SQL Fake takes a different approach - parse and execute SQL SELECT, INSERT, UPDATE, and DELETE queries against an in-memory "database" stored with nested arrays. As long as the amount of data used for testing is small, this solves the problems mentioned above.

## Usage

SQL Fake works by providing a subclass of [AsyncMysqlConnectionPool](https://docs.hhvm.com/hack/reference/class/AsyncMysqlConnectionPool/), the recommended method of querying MySQL built-in to Hack.

This library assumes you currently have some form of establishing a database connection using `AsyncMysqlConnectionPool`. In tests, you can use dependency injection or (`fb_intercept()`)[https://docs.hhvm.com/hack/reference/function/fb_intercept/] to instantiate a `Slack\SQLFake\AsyncMysqlConnectionPool` instead. This will behave like the database for the rest of your test run.

Once per test run, you should also call `Slack\SQLFake\init()` to register database schema.

## Features

- Validating Parser. Queries are validated for SQL syntax, and invalid queries will throw exceptions. Catching exceptions in tests instead of productions is hugely beneficial.
- Optional Schema Parsing. If you provide SQL Fake with a representation of your database schema, it can enforce primary key constraints, nullability, data types, default values, and throw exceptions on any queries referencing columns or tables not in the schema.

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

For an overview of everything that's supported, see the `tests/` for this library, which document nearly all of the SQL features it supports.

## Why doesn't it support X?

This library aims to support everything its authors and users use in MySQL, rather than every possibly feature MySQL offers. We welcome pull requests to add support for new syntax, sql functions, bug fixes, and other features. See our #issues page for a wishlist.

## Other Features

- `snapshot()` and `restore()` functions enable restoring your database to a known state to create isolation between test cases
- `Metrics` class contains information about which queries were run during your tests, and can be used to track the counts of queries executed over time in a test suite.
- Server configuration

## Schema Dumps

It's possible to use this library without providing a schema dump, but many of its most important safety features such as constraint validation and data type validation require schema to function.

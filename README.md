# Hack SQL Fake

[![Build Status](https://travis-ci.org/slackhq/hack-sql-fake.svg?branch=master)](https://travis-ci.org/slackhq/hack-sql-fake)

Hack SQL Fake is unit testing library for [Hack](https://hacklang.org/). It enables testing database-driven applications with an in-memory simulation of MySQL. It supports a wide variety of queries, rapid snapshot/restore of the database between test cases, and more. This is done with a [Fake Object](https://martinfowler.com/bliki/TestDouble.html), which contains an implementation of the database, avoiding the need for explicit stubbing or mocking.

## Motivation

In most unit testing libraries, SQL queries are traditionally replaced with Mock or Stub implementations. Mocks require an explicit list of queries that are expected to run and results to return, while stubs may not even check the queries being run and simply return a hard coded result. This leads to significant manual work setting up expectations, and tests which are fragile and must be updated even on benign changes to the code or queries. It also means the data access layer is not unit tested.

Another common strategy is to test using an actual database, such as SQLite. This creates a situation in which the database in tests may not match the behavior of the production database, and any code using specialized features of the production database may be untestable. It also means that different test cases are not isolated from each other, which can make tests difficult to debug. This can be resolved by truncating tables between each test case, but that can create a performance problem.

Hack SQL Fake takes a different approach - it parses and executes SELECT, INSERT, UPDATE, and DELETE queries against an in-memory "database" stored in hack arrays. As long as the amount of data used for testing is small, this solves the problems mentioned above.

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

SQL Fake works by providing a subclass of [AsyncMysqlConnectionPool](https://docs.hhvm.com/hack/reference/class/AsyncMysqlConnectionPool/), the recommended method of querying MySQL built-in to Hack. A subclass for `AsyncMysqlClient` is provided as well.

This library assumes you currently have some form of establishing a database connection using `AsyncMysqlConnectionPool`. The best way to use SQLFake will depend on your code, but you can use dependency injection or [`fb_intercept()`](https://docs.hhvm.com/hack/reference/function/fb_intercept/) to instantiate a `Slack\SQLFake\AsyncMysqlConnectionPool` when testing. This will behave like a database for the rest of your test run.

Once per test run, you should also call `Slack\SQLFake\init()` to register database schema. See [Exporting Database Schema](#exporting-database-schema) for instructions.

For example, assume you have a class in your source code that manages database connections called `Db` with a method named `getConnectionPool(): AsyncMysqlConnectionPool`. In your tests, you can intercept that function to return an instance of SQLFake's connection pool.

```
$pool = new Slack\SQLFake\AsyncMysqlConnectionPool(darray[]);

fb_intercept('Db::getConnectionPool', (string $name, mixed $obj, array<mixed> $args, mixed $data, bool &$done) ==> {
	$done = true;
	return $pool;
  };
}
```

The rest of your code can operate as normal, using the database in the same way it is used in production.

## Setup and Teardown

You can use the `Slack\SQLFake\snapshot($name);` and `Slack\SQLFake\restore($name);` functions to make snapshots and restore those snapshots. This can help sharing setup between test cases while isolating database modifications the tests make from other tests. If using HackTest, you may want to call `snapshot()` in `beforeFirstTestAsync` and `restore()` in `beforeEachTestAsync()`.

## Exporting Database Schema

By default, SQL Fake will allow you to summon arbitrary tables into existence with insert statements, without having schema information. However, the library includes a schema exporter which will generate Hack code from `.sql` files. This allows SQL Fake to be much more rigorous, improving the value of tests:

- Validate data types, including nullability
- Fail queries against tables that don't exist
- Fail queries against columns that don't exist
- Enforce primary key and unique constraints
- Implement INSERT ... ON DUPLICATE KEY UPDATE

### Dumping schema to `*.sql` files

The first step to utilizing schema is to create one or sql files containing your schema. You should create one per database (possibly multiple per server) using commands like this:

```
mysqldump -d -u someuser -p mydatabase > mydatabase.sql
mysqldump -d -u someuser -p mydatabase2 > mydatabase2.sql
```

### Generating hack schemas from `*.sql` files

Pass all of the previously-generated SQL files as arguments to `bin/hack-sql-schema` and output the results to any Hack file.

```
bin/hack-sql-schema mydatabase.sql mydatabase2.sql > schema.hack
```

This will generate a file containing a single `const` representing your database schema. You can put this file anywhere you want in your codebase, as the constant it contains can be autoloaded. The file names of the sql files will be used as **database** names, with a shape representing the schema for each table in that database.

### Passing schema to SQLFake

The intended use case for schema is to generate a constant containing the schema and assign it to SQL Fake at startup - but you can choose to do assign it at any time with any value. Assuming you generate a constant using the above scripts named `DB_SCHEMA`, you would assign it to SQLFake like this:

```
Slack\SQLFake\init(DB_SCHEMA);
```

This makes SQLFake aware of which tables and columns exist in which databases. You'll also want to tell it which **servers** contain this information.

## Server configuration

MySQL connections operate on a **host or ip**. Since SQLFake operates at the same level as `AsyncMysqlClient`, it also organizes data by hostname. We recommend using fake hostnames in tests that bear some relationship to the production database infrastructure.

At any time, you can tell SQL Fake about which servers exist and provide settings for those by hostname. Any attempt to connect to servers which haven't been explicitly defined will result in an exception.

```php
Slack\SQLFake\add_server("fakehost1", shape(
	// SQL Fake can do some minimal differentiation between 5.6 and 5.7 hosts
	'mysql_version' => '5.6',
	// SQL Fake can further validate queries that are supported by Vitess databases
	'is_vitess' => false,
	// On invalid data types, should SQLFake coerce to a valid type or throw? Similar to how MySQL strict mode behaves
	'strict_sql_mode' => true,
	// If this setting is false, queries that access tables not defined in the schema are allowed
	'strict_schema_mode' => true,
	// Identify the database name in the schema dictionary to use schema from here
	'inherit_schema_from' => 'db1',
));
```

## Metrics

SQL Fake is able to gather metrics about the queries that run during tests, and optionally include stack traces on where those queries ran. This is useful if you'd like to track how many and what kinds of queries are run in key sections of code. This can use a lot of memory, so it is disabled by default.

To enable metrics gathering, set `Slack\SQLFake\Metrics::$enable = true;`. You can then call:

```php
// get the total count of queries that have been invoked
Slack\SQLFake\Metrics::getTotalQueryCount();

// get details on each query that was run
Slack\SQLFake\Metrics::getQueryMetrics();

// get counts by type (insert, update, select, delete)
Slack\SQLFake\Metrics::getCountByQueryType();
```

### Callstacks

When recording information about queries that were invoked in a test, you can capture the callstack at the time the query was run. This can help clarify where those queries are coming from and can be useful to report on which code paths trigger the most queries. To enable callstacks, provide `SQLFake\Metrics::$enableCallstacks = true;`. SQLFake will automatically filter its own functions out of the end of these callstacks. You can also filter out your own low level library code, ensuring the callstacks are most readable, by passing a keyset of patterns to ignore:

```php
Slack\SQLFake\Metrics::\$stackIgnorePatterns = keyset['mysql_*', 'db_*', 'log_*', '_log_*'];
```

Any function names matching those patterns will be removed from the end of the stack trace.

## Why doesn't it support `X`?

This library aims to support everything its users use in MySQL, rather than every possibly feature MySQL offers. We welcome pull requests to add support for new syntax, sql functions, data types, bug fixes, and other features. See our #issues page for a wishlist.

## Contributing

See [Contributing](CONTRIBUTING.md)

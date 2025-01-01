# PHP MySQL Engine

## Overview
PHP MySQL Engine is a library that simulates MySQL 5.6 in-memory for testing database-driven PHP applications. This fork adds new functionality to further enhance testing capabilities while maintaining simplicity and ease of use.

This library extends the PDO class, enabling you to call common PDO MySQL methods and execute a wide variety of queries, including SELECT, INSERT, UPDATE, DELETE, and more.

## Key Features
- **In-Memory Testing**: Simulates MySQL databases with in-memory data for fast and efficient testing.
- **Wide Query Support**: Supports a range of SQL queries, including JOINs, subqueries, UNIONs, and more.
- **PDO Compatibility**: Implements PDO-specific functionality, including transactions, fetch modes, and prepared statements.
- **Validating Parser**: Helps catch SQL syntax errors during testing.

## Motivation
Testing database-driven applications often involves two traditional approaches:

1. **Mock SQL Execution**: Fragile and requires significant manual effort.
2. **Using a Real Database**: Performance-heavy and requires careful management of test data.

PHP MySQL Engine solves these issues by using PHP arrays to simulate an in-memory database, enabling fast, lightweight, and robust testing for database interactions.

## New Additions in This Fork
- MAKETIME
- TIMESTAMP

## SQL Syntax Supported
The library supports:
- **Query Clauses**: FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT.
- **JOINs**: All join types (INNER, LEFT, RIGHT, etc.).
- **Subqueries and Multi-Queries**: UNION, UNION ALL, INTERSECT, EXCEPT.
- **Complex Expressions**: CASE, BETWEEN, row comparators.
- **SQL Functions**: COUNT(), NULLIF(), COALESCE(), CONCAT_WS(), and more.
- **INSERT Features**: INSERT ... ON DUPLICATE KEY UPDATE.
- **Temporary Variables**: e.g., `@var := value`.

## Unsupported Features
- MySQL Stored Objects: Stored procedures, triggers, and views.

## Installation
Install the library using Composer:

```bash
composer require --dev your-namespace/php-mysql-engine
```

## Usage
To use PHP MySQL Engine, instantiate the provided subclass of PDO and inject it into your application:

```php
$pdo = new \YourNamespace\MysqlEngine\Php8\FakePdo($dsn, $user, $password);
$pdo->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
$pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);
```

The rest of your application can interact with the database as usual.

## Contributing
This library focuses on supporting commonly used MySQL features. Contributions to add new features or syntax are welcome.
Contributions are encouraged to expand functionality or improve the library. To contribute:

1. Fork the repository.
2. Add your features or fixes.
3. Ensure all tests pass:

    ```bash
    vendor/bin/phpunit
    vendor/bin/psalm
    ```
4. Submit a pull request.

## Known Issues
- **Result Types**: By default, all data is returned as strings. Setting `\PDO::ATTR_EMULATE_PREPARES` to `false` allows type inference, which may differ slightly from real MySQL behavior.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

---

**Disclaimer:** Use this library at your own risk. It is designed for projects with significant testing needs and maintainers who understand the tradeoffs of using an unofficial MySQL implementation.

---

Thank you for using PHP MySQL Engine! Happy testing!

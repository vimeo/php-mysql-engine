<?php

namespace MysqlEngine;

use Exception;
use PDO;
use PDOStatement;

trait FakePdoStatementTrait
{
    /**
     * @var string
     */
    private $sql;

    /**
     * @var int
     */
    private $affectedRows = 0;

    /**
     * @var array<int, array<string, mixed>>|null
     */
    private $result = null;

    /**
     * @var int
     */
    private $resultCursor = 0;

    /**
     * @var int
     */
    private $fetchMode = PDO::FETCH_BOTH;

    private $fetchArgument;

    /**
     * @var ?array
     */
    private $fetchConstructorArgs = null;

    /**
     * @var FakePdoInterface
     */
    private $conn;

    /**
     * @var ?PDO
     */
    private $real;

    /**
     * @var ?PDOStatement
     */
    private $realStatement = null;

    /**
     * @var array<string|int, scalar>
     */
    private $boundValues = [];

    public function __construct(FakePdoInterface $conn, string $sql, ?PDO $real)
    {
        $this->sql = $sql;
        $this->conn = $conn;
        $this->real = $real;

        if ($this->real) {
            $this->realStatement = $this->real->prepare($this->sql);
        }
    }

    /**
     * @param string|int $key
     * @param scalar $value
     * @param int $type
     * @return bool
     */
    public function bindValue($key, $value, $type = PDO::PARAM_STR): bool
    {
        if (\is_string($key) && $key[0] !== ':') {
            $key = ':' . $key;
        } elseif (\is_int($key)) {
            // Parameter offsets start at 1, which is weird.
            --$key;
        }

        $this->boundValues[$key] = $value;

        if ($this->realStatement) {
            return $this->realStatement->bindValue($key, $value, $type);
        }
        return true;
    }

    /**
     * @param string|int $key
     * @param scalar $value
     * @param int $type
     * @param int $maxLength
     * @param mixed $driverOptions
     * @return bool
     */
    public function bindParam($key, &$value, $type = PDO::PARAM_STR, $maxLength = null, $driverOptions = null): bool
    {
        if (\is_string($key) && $key[0] !== ':') {
            $key = ':' . $key;
        } elseif (\is_int($key)) {
            // Parameter offsets start at 1, which is weird.
            --$key;
        }
        $this->boundValues[$key] = &$value;
        if ($this->realStatement) {
            /**
             * @psalm-suppress PossiblyNullArgument
             */
            return $this->realStatement->bindParam($key, $value, $type, $maxLength, $driverOptions);
        }
        return true;
    }

    /**
     * @param array|null $params
     * @return bool
     * @throws Processor\ProcessorException
     * @throws Processor\SQLFakeUniqueKeyViolation
     */
    public function universalExecute(?array $params = null): bool
    {
        $queries = explode(';', $this->sql);
        $res = [];
        foreach ($queries as $query) {
            $query = trim($query);
            if (empty($query)) {
                continue;
            }
            $res[] = (int)$this->universalExecute_($query, $params);
        }
        $unique = array_flip(array_flip($res));

        return count($unique) !== 1 ? false : (bool)$unique[0];
    }

    /**
     * @param string $sql
     * @param array|null $params
     * @return bool
     * @throws Processor\ProcessorException
     * @throws Processor\SQLFakeUniqueKeyViolation
     */
    public function universalExecute_(string $sql, ?array $params = null): bool
    {
        if ($this->realStatement && $this->realStatement->execute($params) === false) {
            echo $sql;
            throw new \UnexpectedValueException((string)$this->realStatement->errorInfo()[2]);
        }

        if (stripos($sql, 'CREATE TABLE') !== false) {
            $createQueries = (new Parser\CreateTableParser())->parse($sql);

            foreach ($createQueries as $createQuery) {
                if (strpos($createQuery->name, '.')) {
                    [$databaseName, $tableName] = explode('.', $createQuery->name, 2);
                } else {
                    $databaseName = $this->conn->getDatabaseName();
                    $tableName = $createQuery->name;
                }
                $this->conn->getServer()->addTableDefinition(
                    $databaseName,
                    $tableName,
                    Processor\CreateProcessor::makeTableDefinition(
                        $createQuery,
                        $databaseName
                    )
                );
            }
            return true;
        }

        try {
            $parsedQuery = Parser\SQLParser::parse($sql);
        } catch (Parser\LexerException $parse_exception) {
            throw new \UnexpectedValueException(
                'The SQL code ' . $sql . ' could not be converted to parser tokens: ' . $parse_exception->getMessage(),
                0,
                $parse_exception
            );
        } catch (Parser\ParserException $parse_exception) {
            throw new \UnexpectedValueException(
                'The SQL code ' . $sql . ' could not be parsed: ' . $parse_exception->getMessage(),
                0,
                $parse_exception
            );
        }

        $this->result = null;
        $this->resultCursor = 0;
        $this->affectedRows = 0;

        switch (get_class($parsedQuery)) {
            case Query\SelectQuery::class:
                try {
                    $raw_result = Processor\SelectProcessor::process(
                        $this->conn,
                        new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                        $parsedQuery
                    );
                } catch (Processor\ProcessorException $runtime_exception) {
                    throw new \UnexpectedValueException(
                        'The SQL code ' . $sql . ' could not be evaluated: ' . $runtime_exception->getMessage(),
                        0,
                        $runtime_exception
                    );
                }

                $this->result = self::processResult($this->conn, $raw_result);

                if ($this->realStatement) {
                    $fake_result = $this->result;
                    $real_result = $this->realStatement->fetchAll(PDO::FETCH_ASSOC);

                    if ($fake_result) {
                        if ($this->conn->shouldStringifyResult()) {
                            $fake_result = array_map(
                                function ($row) {
                                    return self::stringify($row);
                                },
                                $fake_result
                            );
                        }

                        if ($this->conn->shouldLowercaseResultKeys()) {
                            $fake_result = array_map(
                                function ($row) {
                                    return self::lowercaseKeys($row);
                                },
                                $fake_result
                            );
                        }
                    }

                    if ($real_result != $fake_result) {
                        var_dump($this->getExecutedSql($this->boundValues), $real_result, $fake_result);
                        throw new \TypeError('different');
                    }
                }

                break;

            case Query\InsertQuery::class:
                $this->affectedRows = Processor\InsertProcessor::process(
                    $this->conn,
                    new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                    $parsedQuery
                );

                break;

            case Query\UpdateQuery::class:
                $this->affectedRows = Processor\UpdateProcessor::process(
                    $this->conn,
                    new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                    $parsedQuery
                );

                break;

            case Query\DeleteQuery::class:
                $this->affectedRows = Processor\DeleteProcessor::process(
                    $this->conn,
                    new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                    $parsedQuery
                );

                break;

            case Query\TruncateQuery::class:
                [$databaseName, $tableName] = Processor\Processor::parseTableName($this->conn, $parsedQuery->table);
                $this->conn->getServer()->resetTable($databaseName, $tableName);

                break;

            case Query\DropTableQuery::class:
                [$databaseName, $tableName] = Processor\Processor::parseTableName($this->conn, $parsedQuery->table);
                $this->conn->getServer()->dropTable($databaseName, $tableName);

                break;

            case Query\ShowTablesQuery::class:
                if ($this->conn->getServer()->getTable(
                    $this->conn->getDatabaseName(),
                    $parsedQuery->pattern
                )) {
                    $this->result = [[$parsedQuery->sql => $parsedQuery->pattern]];
                } else {
                    $this->result = [];
                }

                break;

            case Query\ShowIndexQuery::class:
                $this->result = self::processResult(
                    $this->conn,
                    Processor\ShowIndexProcessor::process(
                        $this->conn,
                        new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                        $parsedQuery
                    )
                );
                break;

            case Query\ShowColumnsQuery::class:
                $this->result = self::processResult(
                    $this->conn,
                    Processor\ShowColumnsProcessor::process(
                        $this->conn,
                        new Processor\Scope(array_merge($params ?? [], $this->boundValues)),
                        $parsedQuery
                    )
                );

                break;

            case Query\AlterTableAutoincrementQuery::class:
                [$databaseName, $tableName] = Processor\Processor::parseTableName($this->conn, $parsedQuery->table);
                $td = $this->conn->getServer()->getTableDefinition($databaseName, $tableName);
                if (is_null($td)) {
                    throw new \UnexpectedValueException('Unsupported operation type ' . $sql);
                }
                foreach ($td->columns as $columnName => $column) {
                    if ($column instanceof Schema\Column\IntegerColumn && $column->isAutoIncrement()) {
                        $td->autoIncrementOffsets[$columnName] = (int)$parsedQuery->value - 1;
                    }
                }
                break;
            default:
                throw new \UnexpectedValueException('Unsupported operation type ' . $sql);
        }

        return true;
    }

    /**
     * @psalm-return array<int, array<string, mixed>>
     */
    private static function processResult(FakePdoInterface $conn, Processor\QueryResult $raw_result): array
    {
        $result = [];

        foreach ($raw_result->rows as $i => $row) {
            foreach ($row as $key => $value) {
                /**
                 * @psalm-suppress MixedAssignment
                 */
                $result[$i][\substr($key, 0, 255) ?: ''] = \array_key_exists($key, $raw_result->columns)
                    ? DataIntegrity::coerceValueToColumn($conn, $raw_result->columns[$key], $value)
                    : $value;
            }
        }

        return $result;
    }

    public function columnCount(): int
    {
        if (!$this->result) {
            return 0;
        }

        return \count(\reset($this->result));
    }

    public function rowCount(): int
    {
        return $this->affectedRows;
    }

    /**
     * @param int $fetchMode
     * @param int $cursorOrientation
     * @param int $cursor_offset
     * @return array|false|mixed|null[]|object|string[]|null
     * @throws Exception
     */
    public function fetch(
        $fetchMode = -123,
        $cursorOrientation = PDO::FETCH_ORI_NEXT,
        $cursor_offset = 0
    ) {
        if ($fetchMode === -123) {
            $fetchMode = $this->fetchMode;
        }

        $row = $this->result[$this->resultCursor + $cursor_offset] ?? null;

        if ($row === null) {
            return false;
        }

        if ($this->conn->shouldStringifyResult()) {
            $row = self::stringify($row);
        }

        if ($this->conn->shouldLowercaseResultKeys()) {
            $row = self::lowercaseKeys($row);
        }

        if ($fetchMode === PDO::FETCH_ASSOC) {
            $this->resultCursor++;

            return $row;
        }

        if ($fetchMode === PDO::FETCH_NUM) {
            $this->resultCursor++;

            return \array_values($row);
        }

        if ($fetchMode === PDO::FETCH_COLUMN) {
            $this->resultCursor++;

            return \array_values($row)[0] ?? null;
        }

        if ($fetchMode === PDO::FETCH_BOTH) {
            $this->resultCursor++;

            return array_merge($row, \array_values($row));
        }

        if ($fetchMode === PDO::FETCH_CLASS) {
            $this->resultCursor++;

            return self::convertRowToObject($row, $this->fetchArgument, $this->fetchConstructorArgs);
        }

        throw new Exception('not implemented');
    }

    /**
     * @param int $column
     * @return null|scalar
     */
    public function fetchColumn($column = 0)
    {
        /** @var array<int, scalar>|false $row */
        $row = $this->fetch(PDO::FETCH_NUM);
        if ($row === false) {
            return $row;
        }
        if (!\array_key_exists($column, $row)) {
            throw new \PDOException('SQLSTATE[HY000]: General error: Invalid column index');
        }

        return $row[$column] ?? null;
    }

    /**
     * @param int $fetchMode
     * @param mixed ...$args
     * @return array
     * @throws Exception
     */
    public function universalFetchAll(int $fetchMode = -123, ...$args): array
    {
        if ($fetchMode === -123) {
            $fetchMode = $this->fetchMode;
            $fetch_argument = $this->fetchArgument;
            $ctor_args = $this->fetchConstructorArgs;
        } else {
            $fetch_argument = $args[0] ?? null;
            $ctor_args = $args[1] ?? [];
        }

        if ($fetchMode === PDO::FETCH_ASSOC) {
            return array_map(
                function ($row) {
                    if ($this->conn->shouldStringifyResult()) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->shouldLowercaseResultKeys()) {
                        $row = self::lowercaseKeys($row);
                    }

                    return $row;
                },
                $this->result ?: []
            );
        }

        if ($fetchMode === PDO::FETCH_NUM) {
            return array_map(
                function ($row) {
                    if ($this->conn->shouldStringifyResult()) {
                        $row = self::stringify($row);
                    }

                    return \array_values($row);
                },
                $this->result ?: []
            );
        }

        if ($fetchMode === PDO::FETCH_BOTH) {
            return array_map(
                function ($row) {
                    if ($this->conn->shouldStringifyResult()) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->shouldLowercaseResultKeys()) {
                        $row = self::lowercaseKeys($row);
                    }

                    return array_merge($row, \array_values($row));
                },
                $this->result ?: []
            );
        }

        if ($fetchMode === PDO::FETCH_COLUMN && $fetch_argument !== null) {
            return \array_column(
                array_map(
                    function ($row) {
                        if ($this->conn->shouldStringifyResult()) {
                            $row = self::stringify($row);
                        }

                        return \array_values($row);
                    },
                    $this->result ?: []
                ),
                $fetch_argument
            );
        }

        if ($fetchMode === PDO::FETCH_CLASS) {
            if (!$this->result) {
                return [];
            }

            return array_map(
                function ($row) use ($fetch_argument, $ctor_args) {
                    if ($this->conn->shouldStringifyResult()) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->shouldLowercaseResultKeys()) {
                        $row = self::lowercaseKeys($row);
                    }

                    return self::convertRowToObject($row, $fetch_argument, $ctor_args);
                },
                $this->result
            );
        }
        if ($fetchMode === PDO::FETCH_OBJ) {
            $this->affectedRows++;
            return [(object)$this->result];
        }

        throw new Exception('Fetch style not implemented');
    }

    /**
     * @param int $fetch_style
     * @param mixed $args
     */
    public function universalSetFetchMode(int $mode, ...$args): bool
    {
        $fetch_argument = $args[0] ?? null;
        $ctorargs = $args[1] ?? [];

        if ($this->realStatement) {
            $this->realStatement->setFetchMode($mode, $fetch_argument, $ctorargs);
        }

        $this->fetchMode = $mode;
        $this->fetchArgument = $fetch_argument;
        $this->fetchConstructorArgs = $ctorargs;

        return true;
    }

    /**
     * @param array<string, mixed> $row
     * @return object
     */
    private static function convertRowToObject(array $row, string $class, array $ctor_args)
    {
        $reflector = new \ReflectionClass($class);

        $instance = $reflector->newInstanceWithoutConstructor();

        foreach ($row as $key => $value) {
            if ($key[0] === '`') {
                $key = \substr($key, 1, -1);
            }

            $property = $reflector->getProperty($key);
            $property->setAccessible(true);
            $property->setValue($instance, $value);
            $property->setAccessible(false);
        }

        $instance->__construct(...$ctor_args);

        return $instance;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @psalm-return array<string, null|string>
     */
    private static function stringify(array $row): array
    {
        return \array_map(
            function ($value) {
                return $value === null ? $value : (string)$value;
            },
            $row
        );
    }

    /**
     * @template T
     * @param array<string, T> $row
     *
     * @psalm-return array<string, T>
     */
    private static function lowercaseKeys(array $row): array
    {
        $lowercased_row = [];

        foreach ($row as $col => $value) {
            $lowercased_row[\strtolower($col)] = $value;
        }

        return $lowercased_row;
    }

    /**
     * @psalm-taint-sink callable $class
     *
     * @template T
     * @param class-string<T>|null $class
     * @param array|null $ctorArgs
     * @return   false|T
     */
    public function universalFetchObject(?string $class = \stdClass::class, ?array $ctorArgs = null)
    {
        throw new Exception('not implemented');
    }

    private function getExecutedSql(?array $params): string
    {
        if (!$params) {
            return $this->sql;
        }

        $sql = $this->sql;

        foreach ($params as $key => $value) {
            if (\is_string($key) && $key[0] === ':') {
                $key = \substr($key, 1);
            }

            $sql = preg_replace(
                '/:' . $key . '(?![a-z_A-Z0-9])/',
                \is_string($value) || \is_object($value)
                    ? "'" . str_replace("'", "\\'", (string)$value) . "'"
                    : ($value === null
                    ? 'NULL'
                    : ($value === true
                        ? 'TRUE'
                        : ($value === false
                            ? 'FALSE'
                            : (string)$value
                        )
                    )
                ),
                $sql
            );
        }

        return $sql;
    }

    /**
     * @return array{0: null|string, 1: int|null, 2: null|string, 3?: mixed, 4?: mixed}
     */
    public function errorInfo(): array
    {
        return ['00000', 0, 'PHP MySQL Engine: errorInfo() not supported.'];
    }
}

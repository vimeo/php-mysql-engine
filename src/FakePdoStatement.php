<?php
namespace Vimeo\MysqlEngine;

class FakePdoStatement extends \PDOStatement
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
     * @var ?array
     */
    private $result = null;

    /**
     * @var int
     */
    private $resultCursor = 0;

    /**
     * @var int
     */
    private $fetchMode = \PDO::ATTR_DEFAULT_FETCH_MODE;

    private $fetchArgument;

    /**
     * @var ?array
     */
    private $fetchConstructorArgs = null;

    /**
     * @var FakePdo
     */
    private $conn;

    /**
     * @var ?\PDO
     */
    private $real;

    /**
     * @var ?\PDOStatement
     */
    private $realStatement = null;

    /**
     * @var array<string, scalar>
     */
    private $boundValues = [];

    public function __construct(FakePdo $conn, string $sql, ?\PDO $real)
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
     */
    public function bindValue($key, $value, $type = \PDO::PARAM_STR) : void
    {
        $this->boundValues[$key] = $value;

        if ($this->realStatement) {
            $this->realStatement->bindValue($key, $value, $type);
        }
    }

    /**
     * Overriding execute method to add query logging
     *
     * @return bool
     */
    public function execute(?array $params = null)
    {
        $sql = $this->getExecutedSql($this->boundValues ?: $params);

        if ($this->realStatement) {
            if ($this->realStatement->execute($params) === false) {
                var_dump($this->sql);
                throw new \UnexpectedValueException($this->realStatement->errorInfo()[2]);
            }
        }

        //echo "\n" . $sql . "\n";

        if (stripos($sql, 'CREATE TABLE') !== false
            || stripos($sql, 'DROP TABLE') !== false
            || stripos($sql, 'SHOW TABLES') !== false
        ) {
            $parsed_query = new \PhpMyAdmin\SqlParser\Parser($sql);

            if ($parsed_query->errors) {
                throw $parsed_query->errors[0];
            }

            if (!isset($parsed_query->statements[0])) {
                throw new \UnexpectedValueException('Bad query ' . $sql);
            }

            $statement = $parsed_query->statements[0];

            switch (get_class($statement)) {
                case \PhpMyAdmin\SqlParser\Statements\CreateStatement::class:
                    Processor\CreateProcessor::process($this->conn, $statement);
                    break;

                case \PhpMyAdmin\SqlParser\Statements\DropStatement::class:
                    $this->conn->getServer()->resetTable(
                        $this->conn->databaseName,
                        $statement->fields[0]->table
                    );
                    break;
                case \PhpMyAdmin\SqlParser\Statements\ShowStatement::class:
                    if (count($statement->unknown) === 7
                        && $statement->unknown[4]->value === 'LIKE'
                    ) {
                        if ($this->conn->getServer()->getTable(
                            $this->conn->databaseName,
                            $statement->unknown[4]->value
                        )) {
                            $this->result = [[$statement->unknown[4]->value]];
                        }

                        $this->result = [];

                        return true;
                    }
            }

            return true;
        }

        $parsed_query = Parser\SQLParser::parse($sql);

        $this->result = null;
        $this->resultCursor = 0;
        $this->affectedRows = 0;

        switch (get_class($parsed_query)) {
            case Query\SelectQuery::class:
                $this->result = Processor\SelectProcessor::process(
                    $this->conn,
                    $parsed_query,
                    null
                );

                if ($this->realStatement) {
                    $fake_result = $this->result;
                    $real_result = $this->realStatement->fetchAll(\PDO::FETCH_ASSOC);

                    if ($fake_result) {
                        if ($this->conn->stringifyResult) {
                            $fake_result = array_map(
                                function ($row) {
                                    return self::stringify($row);
                                },
                                $fake_result
                            );
                        }

                        if ($this->conn->lowercaseResultKeys) {
                            $fake_result = array_map(
                                function ($row) {
                                    return self::lowercaseKeys($row);
                                },
                                $fake_result
                            );
                        }
                    }

                    if ($real_result !== $fake_result) {
                        var_dump($real_result, $fake_result);
                        throw new \UnexpectedValueException('different');
                    }
                }

                break;

            case Query\InsertQuery::class:
                $this->affectedRows = Processor\InsertProcessor::process($this->conn, $parsed_query);
                break;

            case Query\UpdateQuery::class:
                $this->affectedRows = Processor\UpdateProcessor::process($this->conn, $parsed_query);
                break;

            case Query\DeleteQuery::class:
                $this->affectedRows = Processor\DeleteProcessor::process($this->conn, $parsed_query);
                break;

            case Query\TruncateQuery::class:
                $this->conn->getServer()->resetTable(
                    $this->conn->databaseName,
                    $parsed_query->table
                );
                break;

            default:
                throw new \UnexpectedValueException('Unsupported operation type ' . $sql);
        }

        return true;
    }

    public function columnCount() : int
    {
        if (!$this->result) {
            return 0;
        }

        return \count(\reset($this->result));
    }

    public function rowCount() : int
    {
        return $this->affectedRows;
    }

    public function fetch(
        int $fetch_style = -123,
        int $cursor_orientation = \PDO::FETCH_ORI_NEXT,
        int $cursor_offset = 0
    ) {
        if ($fetch_style === -123) {
            $fetch_style = $this->fetchMode;
        }

        $row = $this->result[$this->resultCursor + $cursor_offset] ?? null;

        if ($row === null) {
            return false;
        }

        if ($this->conn->stringifyResult) {
            $row = self::stringify($row);
        }

        if ($this->conn->lowercaseResultKeys) {
            $row = self::lowercaseKeys($row);
        }

        if ($fetch_style === \PDO::FETCH_ASSOC) {
            $this->resultCursor++;

            return $row;
        }

        if ($fetch_style === \PDO::FETCH_NUM) {
            $this->resultCursor++;

            return \array_values($row);
        }

        if ($fetch_style === \PDO::FETCH_BOTH) {
            $this->resultCursor++;

            return array_merge($row, \array_values($row));
        }

        if ($fetch_style === \PDO::FETCH_CLASS) {
            $this->resultCursor++;

            return self::convertRowToObject($row, $this->fetchArgument, $this->fetchConstructorArgs);
        }

        throw new \Exception('not implemented');
    }

    /**
     * @param  int $fetch_style
     * @param  mixed      $args
     */
    public function fetchAll(int $fetch_style = -123, ...$args) : array
    {
        if ($fetch_style === -123) {
            $fetch_style = $this->fetchMode;
            $fetch_argument = $this->fetchArgument;
            $ctor_args = $this->fetchConstructorArgs;
        } else {
            $fetch_argument = $args[0] ?? null;
            $ctor_args = $args[1] ?? [];
        }

        if ($fetch_style === \PDO::FETCH_ASSOC) {
            return array_map(
                function ($row) {
                    if ($this->conn->stringifyResult) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->lowercaseResultKeys) {
                        $row = self::lowercaseKeys($row);
                    }

                    return $row;
                },
                $this->result ?: []
            );

            return $this->result ?: [];
        }

        if ($fetch_style === \PDO::FETCH_NUM) {
            return array_map(
                function ($row) {
                    if ($this->conn->stringifyResult) {
                        $row = self::stringify($row);
                    }

                    return \array_values($row);
                },
                $this->result ?: []
            );
        }

        if ($fetch_style === \PDO::FETCH_BOTH) {
            return array_map(
                function ($row) {
                    if ($this->conn->stringifyResult) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->lowercaseResultKeys) {
                        $row = self::lowercaseKeys($row);
                    }

                    return array_merge($row, \array_values($row));
                },
                $this->result ?: []
            );
        }

        if ($fetch_style === \PDO::FETCH_COLUMN && $fetch_argument !== null) {
            return \array_column(
                array_map(
                    function ($row) {
                        if ($this->conn->stringifyResult) {
                            $row = self::stringify($row);
                        }

                        return \array_values($row);
                    },
                    $this->result ?: []
                ),
                $fetch_argument
            );
        }

        if ($fetch_style === \PDO::FETCH_CLASS) {
            if (!$this->result) {
                return [];
            }

            return array_map(
                function ($row) use ($fetch_argument, $ctor_args) {
                    if ($this->conn->stringifyResult) {
                        $row = self::stringify($row);
                    }

                    if ($this->conn->lowercaseResultKeys) {
                        $row = self::lowercaseKeys($row);
                    }

                    return self::convertRowToObject($row, $fetch_argument, $ctor_args);
                },
                $this->result
            );
        }

        throw new \Exception('Fetch style not implemented');
    }

    /**
     * @param  int $fetch_style
     * @param  mixed      $args
     */
    public function setFetchMode(int $mode, ...$args) : bool
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

    private static function stringify(array $row)
    {
        return \array_map(
            function ($value) {
                return $value === null ? $value : (string) $value;
            },
            $row
        );
    }

    private static function lowercaseKeys(array $row)
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
     * @param    class-string<T> $class
     * @return   false|T
     */
    public function fetchObject(?string $class = \stdClass::class, ?array $ctorArgs = null)
    {
        throw new \Exception('not implemented');
    }

    private function getExecutedSql(?array $params) : string
    {
        if (!$params) {
            return $this->sql;
        }

        $sql = $this->sql;

        foreach ($params as $key => $value) {
            if ($key[0] === ':') {
                $key = \substr($key, 1);
            }

            $sql = preg_replace(
                '/:' . $key . '(?![a-z_A-Z0-9])/',
                \is_string($value) || \is_object($value)
                    ? "'" . str_replace("'", "\\'", (string) $value) . "'"
                    : ($value === null
                        ? 'NULL'
                        : ($value === true
                            ? 'TRUE'
                            : ($value === false
                                ? 'FALSE'
                                : (string) $value
                            )
                        )
                    ),
                $sql
            );
        }

        return $sql;
    }
}

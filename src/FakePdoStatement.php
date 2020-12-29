<?php
namespace Vimeo\MysqlEngine;

class FakePdoStatement extends \PDOStatement
{
    private string $sql;

    private int $affectedRows = 0;

    private ?array $result = null;

    private int $resultCursor = 0;

    private int $fetchMode = \PDO::ATTR_DEFAULT_FETCH_MODE;

    private $fetchArgument;

    private ?array $fetchConstructorArgs = null;

    private FakePdo $conn;

    private ?\PDO $real;

    private ?\PDOStatement $realStatement = null;

    /**
     * @var array<string, scalar>
     */
    private array $boundValues = [];

    public function __construct(FakePdo $conn, string $sql, ?\PDO $real)
    {
        $this->sql = $sql;
        $this->conn = $conn;
        $this->real = $real;
    }

    /**
     * @param string $key
     * @param scalar $value
     */
    public function bindValue(string $key, $value) : void
    {
        $this->boundValues[$key] = $value;
    }

    /**
     * Overriding execute method to add query logging
     *
     * @return bool
     */
    public function execute()
    {
        $sql = $this->getExecutedSql();

        if ($this->real) {
            $this->realStatement = $this->real->prepare($this->sql);

            if ($this->realStatement->execute($this->boundValues) === false) {
                var_dump($this->sql);
                throw new \UnexpectedValueException($this->realStatement->errorInfo()[2]);
            }
        }

        //echo "\n" . $sql . "\n";

        if (stripos($sql, 'CREATE TABLE') !== false || stripos($sql, 'DROP TABLE') !== false) {
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
                    $parsed_query
                );

                if ($this->realStatement) {
                    $fake_result = $this->result;
                    $real_result = $this->realStatement->fetchAll(\PDO::FETCH_ASSOC);

                    if ($this->conn->stringifyResult && $fake_result) {
                        $fake_result = array_map(fn($row) => self::stringify($row), $fake_result);
                    }

                    if ($real_result !== $fake_result) {
                        //var_dump($real_result, $fake_result);
                        //throw new \UnexpectedValueException('different');
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
        ?int $fetch_style = -123,
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

    public function fetchAll(int $fetch_style = -123, $fetch_argument = null, array $ctor_args = []) : array
    {
        if ($fetch_style === -123) {
            $fetch_style = $this->fetchMode;
            $fetch_argument = $this->fetchArgument;
            $ctor_args = $this->fetchConstructorArgs;
        }

        if ($fetch_style === \PDO::FETCH_ASSOC) {
            if ($this->conn->stringifyResult) {
                return array_map(
                    fn($row) => self::stringify($row),
                    $this->result ?: []
                );
            }

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

                    return self::convertRowToObject($row, $fetch_argument, $ctor_args);
                },
                $this->result
            );
        }

        throw new \Exception('Fetch style not implemented');
    }

    public function setFetchMode(int $mode, $fetch_argument = null, array $ctorargs = []) : bool
    {
        if ($this->realStatement) {
            $this->realStatement->setFetchMode($mode, $fetch_argument, $ctorargs);
        }

        $this->fetchMode = $mode;
        $this->fetchArgument = $fetch_argument;
        $this->fetchConstructorArgs = $ctorargs;

        return true;
    }

    private static function convertRowToObject(array $row, string $class, array $ctor_args) : object
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
        return \array_map(fn($value) => $value === null ? $value : (string) $value, $row);
    }

    /**
     * @psalm-taint-sink callable $class
     *
     * @template T
     * @param    class-string<T> $class
     * @return   false|T
     */
    public function fetchObject(string $class = \stdClass::class)
    {
        throw new \Exception('not implemented');
    }

    private function getExecutedSql() : string
    {
        if (!$this->boundValues) {
            return $this->sql;
        }

        $sql = $this->sql;

        foreach ($this->boundValues as $key => $value) {
            if ($key[0] === ':') {
                $key = \substr($key, 1);
            }

            $sql = preg_replace(
                '/:' . $key . '(?![a-z_A-Z0-9])/',
                \is_string($value) || \is_object($value)
                    ? "'" . str_replace("'", "\\'", (string) $value) . "'"
                    : ($value === null ? 'NULL' : $value),
                $sql
            );
        }

        return $sql;
    }
}

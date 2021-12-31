<?php

namespace MysqlEngine\Php8;

use Exception;

class FakePdoStatement extends \PDOStatement
{
    use \MysqlEngine\FakePdoStatementTrait;

    /**
     * Overriding execute method to add query logging
     * @param ?array $params
     * @return bool
     */
    public function execute(?array $params = null): bool
    {
        return $this->universalExecute($params);
    }

    /**
     * @param int $mode
     * @param mixed ...$args
     * @return array
     * @throws Exception
     * @psalm-suppress MethodSignatureMismatch
     */
    public function fetchAll(int $mode = -123, ...$args): array
    {
        return $this->universalFetchAll($mode, ...$args);
    }

    /**
     * @param int $mode <p>
     * @param null|string|object $className
     * @param array $params
     * @return bool
     */
    public function setFetchMode($mode, $className = null, ...$params): bool
    {
        return $this->universalSetFetchMode($mode, ...$params);
    }

    /**
     * @psalm-taint-sink callable $class
     *
     * @template T
     * @param class-string<T>|null $class
     * @param array|null $ctorArgs
     * @return   false|T
     */
    public function fetchObject(?string $class = \stdClass::class, ?array $ctorArgs = null)
    {
        return $this->universalFetchObject($class, $ctorArgs);
    }
}

<?php
namespace MysqlEngine\Php7;

use Exception;
use MysqlEngine\FakePdoStatementTrait;
use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Processor\SQLFakeUniqueKeyViolation;

class FakePdoStatement extends \PDOStatement
{
    use FakePdoStatementTrait;

    /**
     * Overriding execute method to add query logging
     * @param ?array $params
     * @return bool
     * @throws ProcessorException|SQLFakeUniqueKeyViolation
     */
    public function execute($params = null): bool
    {
        return $this->universalExecute($params);
    }

    /**
     * @param int $mode
     * @param mixed $fetch_argument
     * @param mixed $args
     * @return array
     * @throws Exception
     */
    public function fetchAll($mode = -123, $fetch_argument = null, $args = null): array
    {
        return $this->universalFetchAll($mode, $fetch_argument, $args);
    }

    /**
     * @param  int $mode
     * @param  mixed $fetch_argument
     * @param  array $ctorargs
     */
    public function setFetchMode($mode, $fetch_argument = null, $ctorargs = []) : bool
    {
        return $this->universalSetFetchMode($mode, $fetch_argument, $ctorargs);
    }

    /**
     * @psalm-taint-sink callable $class
     *
     * @template T
     * @param    class-string<T>|null $class
     * @param    array|null $ctorArgs
     * @return   false|T
     */
    public function fetchObject($class = \stdClass::class, $ctorArgs = null)
    {
        return $this->universalFetchObject($class, $ctorArgs);
    }
}

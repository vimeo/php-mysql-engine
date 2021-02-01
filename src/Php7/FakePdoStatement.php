<?php
namespace Vimeo\MysqlEngine\Php7;

class FakePdoStatement extends \PDOStatement
{
    use \Vimeo\MysqlEngine\FakePdoTrait;

    /**
     * Overriding execute method to add query logging
     * @param ?array $params
     * @return bool
     */
    public function execute($params = null)
    {
        return $this->universalExecute($params);
    }

    /**
     * @param  int $fetch_style
     * @param  string $fetch_argument
     * @param  array $ctor_args
     */
    public function fetchAll($fetch_style = -123, $fetch_argument = null, $ctor_args = null) : array
    {
        return $this->universalFetchAll($fetch_style, $fetch_argument, $ctor_args);
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

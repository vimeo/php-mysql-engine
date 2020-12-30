<?php
namespace Vimeo\MysqlEngine;

class FakePdo extends \PDO
{
    /**
     * @var Server
     */
    private $server;

    /**
     * @var ?\PDO
     */
    private $real = null;

    /**
     * @var scalar
     */
    public $lastInsertId;

    /**
     * @var bool
     */
    public $stringifyResult = true;

    /**
     * @var ?string
     * @readonly
     */
    public $databaseName = null;

    public function __construct(string $dsn, string $username = '', string $passwd = '', array $options = [])
    {
        //$this->real = new \PDO($dsn, $username, $passwd, $options);

        $dsn = \Nyholm\Dsn\DsnParser::parse($dsn);
        $host = $dsn->getHost();

        if (preg_match('/dbname=([a-zA-Z0-9_]+);/', $host, $matches)) {
            $this->databaseName = $matches[1];
        }

        $this->server = Server::getOrCreate('primary');
    }

    public function setAttribute($key, $value)
    {
        if ($key === \PDO::ATTR_EMULATE_PREPARES) {
            $this->stringifyResult = (bool) $value;
        }

        if ($this->real && $key !== \PDO::ATTR_STATEMENT_CLASS) {
            $this->real->setAttribute($key, $value);
        }
    }

    public function getServer()
    {
        return $this->server;
    }

    /**
     * @param  string $statement
     */
    public function prepare($statement, $options = null)
    {
        return new FakePdoStatement($this, $statement, $this->real);
    }

    public function lastInsertId($seqname = NULL)
    {
        return $this->lastInsertId;
    }

    public function beginTransaction()
    {
        Server::snapshot('transaction');
    }

    public function commit()
    {
        return Server::deleteSnapshot('transaction');
    }

    public function rollback()
    {
        Server::restoreSnapshot('transaction');
    }
}

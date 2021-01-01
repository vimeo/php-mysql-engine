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
     * @var bool
     */
    public $lowercaseResultKeys = false;

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

        if ($key === \PDO::ATTR_CASE && $value === \PDO::CASE_LOWER) {
            $this->lowercaseResultKeys = true;
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

    public function lastInsertId($seqname = null)
    {
        if ($this->real) {
            if ($this->lastInsertId != $this->real->lastInsertId($seqname)) {
                var_dump($this->real->lastInsertId($seqname), $this->lastInsertId);
                throw new \UnexpectedValueException('different last insert id');
            }
        }

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

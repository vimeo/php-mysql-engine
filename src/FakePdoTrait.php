<?php
namespace Vimeo\MysqlEngine;

trait FakePdoTrait
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
     * @var string
     */
    public $lastInsertId = "0";

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
            return $this->real->setAttribute($key, $value);
        }

        return true;
    }

    public function getServer() : Server
    {
        return $this->server;
    }

    public function getDatabaseName() : ?string
    {
        return $this->databaseName;
    }

    public function shouldStringifyResult(): bool
    {
        return $this->stringifyResult;
    }

    public function shouldLowercaseResultKeys(): bool
    {
        return $this->lowercaseResultKeys;
    }

    public function setLastInsertId(string $last_insert_id) : void
    {
        $this->lastInsertId = $last_insert_id;
    }

    public function lastInsertId($seqname = null) : string
    {
        if ($this->real) {
            $real_last_insert_id = $this->real->lastInsertId($seqname);
            if ($this->lastInsertId !== $real_last_insert_id) {
                throw new \UnexpectedValueException(
                    'different last insert id – saw ' . $this->lastInsertId
                        . ' but MySQL produced ' . $real_last_insert_id
                );
            }
        }

        return $this->lastInsertId;
    }

    public function beginTransaction()
    {
        Server::snapshot('transaction');
        return true;
    }

    public function commit()
    {
        return Server::deleteSnapshot('transaction');
    }

    public function rollback()
    {
        Server::restoreSnapshot('transaction');
        return true;
    }

    /**
     * @param string $statement
     * @return int|false
     */
    public function exec($statement)
    {
        $statement = trim($statement);

        if (strpos($statement, 'SET ')===0) {
            return false;
        }

        $sth = $this->prepare($statement);

        if ($sth->execute()) {
            return $sth->rowCount();
        }

        return false;
    }
}

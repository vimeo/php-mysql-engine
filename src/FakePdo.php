<?php
namespace Vimeo\MysqlEngine;

use PDO;

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

	/**
	 * @param string $statement
	 * @return Php7\FakePdoStatement|Php8\FakePdoStatement
	 */
    public function prepare($statement, $options = null)
    {
        if (\PHP_MAJOR_VERSION === 8) {
            return new Php8\FakePdoStatement($this, $statement, $this->real);
        }

        return new Php7\FakePdoStatement($this, $statement, $this->real);
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

	/**
	 * @return bool
	 */
    public function beginTransaction()
    {
        Server::snapshot('transaction');
        return true;
    }

	/**
	 * @return bool
	 */
    public function commit()
    {
        return Server::deleteSnapshot('transaction');
    }

	/**
	 * @return bool
	 * @throws Processor\ProcessorException
	 */
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
		if (strpos($statement, 'SET ')===0){
			return false;
		}

		$sth = $this->prepare($statement);
		if ($sth->execute()){
			return $sth->rowCount();
		}
		return false;
	}

	/**
	 * @param string $statement
	 * @param int $mode
	 * @param null $arg3
	 * @param array $ctorargs
	 * @return Php7\FakePdoStatement|Php8\FakePdoStatement
	 */
	public function query($statement, $mode = PDO::ATTR_DEFAULT_FETCH_MODE, $arg3 = null, array $ctorargs = [])
	{
		$sth = $this->prepare($statement);
		$sth->execute();
		return $sth;
	}
}

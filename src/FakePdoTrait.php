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
     * @var ?\DateTimeZone
     */
    private $timezone = null;

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

    /** @var ?int */
    private $defaultFetchMode = null;

    /**
     * @var bool
     */
    public $strict_mode = false;

    /**
     * @var ?string
     * @readonly
     */
    public $databaseName = null;

    /**
     * @param array<string>  $options
     */
    public function __construct(string $dsn, string $username = '', string $passwd = '', array $options = [])
    {
        //$this->real = new \PDO($dsn, $username, $passwd, $options);

        $dsn = \Nyholm\Dsn\DsnParser::parse($dsn);
        $host = $dsn->getHost();

        if (preg_match('/dbname=([a-zA-Z0-9_]+);/', $host, $matches)) {
            $this->databaseName = $matches[1];
        }

        // do a quick check for this string – hacky but fast
        $this->strict_mode = \array_key_exists(\PDO::MYSQL_ATTR_INIT_COMMAND, $options)
            && \strpos($options[\PDO::MYSQL_ATTR_INIT_COMMAND], 'STRICT_ALL_TABLES');

        $should_set_timezone = \array_key_exists(\PDO::MYSQL_ATTR_INIT_COMMAND, $options)
            && \strpos($options[\PDO::MYSQL_ATTR_INIT_COMMAND], 'SET time_zone = ') !== false;
        if ($should_set_timezone && preg_match('/SET time_zone = \'((?:\\+|-)?\\d{1,2}:\\d{2})\';/', $options[\PDO::MYSQL_ATTR_INIT_COMMAND], $matches)) {
            $this->timezone = new \DateTimeZone($matches[1]);
        }

        $this->server = Server::getOrCreate('primary');
    }

    #[\ReturnTypeWillChange]
    public function setAttribute($key, $value)
    {
        if ($key === \PDO::ATTR_EMULATE_PREPARES) {
            $this->stringifyResult = (bool) $value;
        }

        if ($key === \PDO::ATTR_CASE && $value === \PDO::CASE_LOWER) {
            $this->lowercaseResultKeys = true;
        }

        if ($key === \PDO::ATTR_DEFAULT_FETCH_MODE) {
            if (!is_int($value)) {
                throw new \PDOException("SQLSTATE[HY000]: General error: invalid fetch mode type");
            }
            $this->defaultFetchMode = $value;
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

    public function useStrictMode() : bool
    {
        return $this->strict_mode;
    }

    #[\ReturnTypeWillChange]
    public function beginTransaction()
    {
        if (Server::hasSnapshot('transaction')) {
            return false;
        }

        Server::snapshot('transaction');
        return true;
    }

    #[\ReturnTypeWillChange]
    public function commit()
    {
        return Server::deleteSnapshot('transaction');
    }

    #[\ReturnTypeWillChange]
    public function rollback()
    {
        if (!Server::hasSnapshot('transaction')) {
            return false;
        }

        Server::restoreSnapshot('transaction');
        return true;
    }

    #[\ReturnTypeWillChange]
    public function inTransaction()
    {
        return Server::hasSnapshot('transaction');
    }

    /**
     * @param string $statement
     * @return int|false
     */
    #[\ReturnTypeWillChange]
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

    /**
     * @param string $string
     * @param int $parameter_type
     * @return string
     */
    #[\ReturnTypeWillChange]
    public function quote($string, $parameter_type = \PDO::PARAM_STR)
    {
        // @see https://github.com/php/php-src/blob/php-8.0.2/ext/mysqlnd/mysqlnd_charset.c#L860-L878
        $quoted = strtr($string, [
            "\0" => '\0',
            "\n" => '\n',
            "\r" => '\r',
            "\\" => '\\\\',
            "\'" => '\\\'',
            "\"" => '\\"',
            "\032" => '\Z',
        ]);

        // @see https://github.com/php/php-src/blob/php-8.0.2/ext/pdo_mysql/mysql_driver.c#L307-L320
        $quotes = ['\'', '\''];
        /** @psalm-suppress MixedOperand */
        if (defined('PDO::PARAM_STR_NATL') &&
            (constant('PDO::PARAM_STR_NATL') & $parameter_type) === constant('PDO::PARAM_STR_NATL')
        ) {
            $quotes[0] = 'N\'';
        }

        return "{$quotes[0]}{$quoted}{$quotes[1]}";
    }

    /**
     * @return array{0: null|string, 1: int|null, 2: null|string, 3?: mixed, 4?: mixed}
     */
    public function errorInfo(): array
    {
        return ['00000', 0, 'PHP MySQL Engine: errorInfo() not supported.'];
    }

    public function getTimezone(): ?\DateTimeZone
    {
        return $this->timezone;
    }
}

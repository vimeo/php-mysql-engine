<?php
namespace Vimeo\MysqlEngine;

use PDO;

class FakePdo
{
    /**
     * @param string $connection_string the connection string
     * @param string $username the username
     * @param string $password the password
     * @param array<array-key, string> $options any options
     * @return PDO
     */
    public static function getFakePdo(
        string $connection_string,
        string $username,
        string $password,
        array $options
    ): PDO {
        if (\PHP_MAJOR_VERSION === 8) {
            return new Php8\FakePdo($connection_string, $username, $password, $options);
        }

        return new Php7\FakePdo($connection_string, $username, $password, $options);
    }
}

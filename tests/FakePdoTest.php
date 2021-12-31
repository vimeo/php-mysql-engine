<?php

namespace MysqlEngine\Tests;

use PDO;
use PDOException;

class FakePdoTest extends \PHPUnit\Framework\TestCase
{
    public function tearDown(): void
    {
        \MysqlEngine\Server::reset();
    }

    public function testMinimalTransaction(): void
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;');

        self::assertFalse($pdo->inTransaction());
        $pdo->beginTransaction();
        self::assertTrue($pdo->inTransaction());
        $pdo->commit();
        self::assertFalse($pdo->inTransaction());


        $pdo->beginTransaction();
        self::assertTrue($pdo->inTransaction());
        $pdo->rollBack();
        self::assertFalse($pdo->inTransaction());
    }

    /**
     * @dataProvider quotationStringProvider
     */
    public function testQuote(string $subject, string $expected): void
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;');

        self::assertSame($expected, $pdo->quote($subject));
    }

    /**
     * @return array<string,array{0:string,1:string}>
     */
    public function quotationStringProvider(): array
    {
        return [
            'empty string' => ["", '\'\''],
            'a' => ["a", '\'a\''],
            'Kyoto in Chinese character' => ["京都", '\'京都\''],
            'null character' => ["\0", '\'\0\''],
            'includes newline(LF)'=> ["\na\nb", '\'\na\nb\''],
            'includes newline(CRLF)'=> ["\r\na\r\nb", '\'\r\na\r\nb\''],
            'includes quotations'=> ["\'a\"b", '\'\\\'a\\"b\''],
            'includes ascii 032(\Z)' => [implode(['a', chr(032), 'b']), '\'a\Zb\''],
        ];
    }

    /**
     * @param string $connectionString
     * @param bool $strictMode
     * @return PDO
     */
    private static function getPdo(string $connectionString, bool $strictMode = false) : PDO
    {
        $options = $strictMode ? [PDO::MYSQL_ATTR_INIT_COMMAND => 'SET sql_mode="STRICT_ALL_TABLES"'] : [];

        if (\PHP_MAJOR_VERSION === 8) {
            return new \MysqlEngine\Php8\FakePdo($connectionString, '', '', $options);
        }

        return new \MysqlEngine\Php7\FakePdo($connectionString, '', '', $options);
    }
}

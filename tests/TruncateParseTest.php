<?php

namespace MysqlEngine\Tests;

use MysqlEngine\Parser\ParserException;
use MysqlEngine\Parser\SQLParser;
use MysqlEngine\Parser\Token;
use MysqlEngine\Parser\TruncateParser;
use MysqlEngine\Query\TruncateQuery;
use MysqlEngine\TokenType;

use function array_shift;
use function sprintf;

class TruncateParseTest extends \PHPUnit\Framework\TestCase
{

    public function dataProviderInvalidTokens(): array
    {
        return [
            'empty array'                         => [
                [],
            ],
            'associative array (equals to empty)' => [
                [
                    'a' => new Token(TokenType::CLAUSE, 'SELECT', 'SELECT', 0),
                    'b' => new Token(TokenType::IDENTIFIER, 'table_name', 'table_name', 0),
                ],
            ],
            'select'                              => [
                [
                    new Token(TokenType::CLAUSE, 'SELECT', 'SELECT', 0),
                ],
            ],
            'invalid token type'                  => [
                [
                    new Token(TokenType::CLAUSE, 'TRUNCATE', 'TRUNCATE', 0),
                    new Token(TokenType::RESERVED, 'TRUNCATE', 'TRUNCATE', 0),
                ],
            ],
        ];
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @dataProvider dataProviderInvalidTokens
     */
    public function testInvalidTokens(array $tokens): void
    {
        $parser = new TruncateParser($tokens, '');
        $this->expectException(ParserException::class);
        $parser->parse();
    }

    /**
     * @return array<string, {0: string}>
     */
    public function dataProviderQueryForms(): array
    {
        return [
            'short'       => ['TRUNCATE %s'],
            'long'        => ['TRUNCATE TABLE %s'],
            'lower short' => ['truncate %s'],
            'lower long'  => ['truncate table %s'],
        ];
    }

    /**
     * @param string $form
     *
     * @dataProvider dataProviderQueryForms
     */
    public function testTruncate(string $form): void
    {
        $query = sprintf($form, '`table_name`');

        $truncate_query = SQLParser::parse($query);

        self::assertInstanceOf(TruncateQuery::class, $truncate_query);
    }

    /**
     * @param string $form
     *
     * @dataProvider dataProviderQueryForms
     */
    public function testTruncateTable(string $form): void
    {
        $query = sprintf($form, '`table_name`');

        $truncate_query = SQLParser::parse($query);

        self::assertInstanceOf(TruncateQuery::class, $truncate_query);
    }

    /**
     * @param string $query
     *
     * @dataProvider dataProviderInvalidStatements
     */
    public function testInvalidStatement(string $query): void
    {
        $this->expectException(ParserException::class);

        SQLParser::parse($query);
    }

    public function dataProviderInvalidStatements(): \Generator
    {
        foreach ([
                 // this check uses anything that is NOT a identifier
                 //   will result to `truncate table select`
                'invalid identifier' => 'select',

                 // missing identifier checks
                 //   will result to `truncate table`
                'missing identifier' => '',
            ] as $case => $identifier
        ) {
            foreach ($this->dataProviderQueryForms() as $formName => $sql) {
                yield $case . ': ' . $formName => [sprintf(array_shift($sql), $identifier)];
            }
        }
    }
}

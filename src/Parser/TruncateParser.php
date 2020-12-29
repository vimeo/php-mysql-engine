<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\TruncateQuery;

final class TruncateParser
{
    private int $pointer = 0;

    /**
     * @var array<int, array{type:TokenType::*, value:string, raw:string}>
     */
    private array $tokens;

    private string $sql;

    /**
     * @param array<int, array{type:TokenType::*, value:string, raw:string}> $tokens
     */
    public function __construct(array $tokens, string $sql)
    {
        $this->tokens = $tokens;
        $this->sql = $sql;
    }

    public function parse() : TruncateQuery
    {
        if ($this->tokens[$this->pointer]['value'] !== 'TRUNCATE') {
            throw new SQLFakeParseException("Parser error: expected TRUNCATE");
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer];

        if ($token === null || $token['type'] !== TokenType::IDENTIFIER) {
            throw new SQLFakeParseException("Expected table name after TRUNCATE");
        }

        return new TruncateQuery($token['value'], $this->sql);
    }
}

<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\ShowTablesQuery;

/**
 * Very limited parser for SHOW TABLES LIKE 'foo'
 */
final class ShowParser
{
    /**
     * @var int
     */
    private $pointer = 0;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @var string
     */
    private $sql;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(array $tokens, string $sql)
    {
        $this->tokens = $tokens;
        $this->sql = $sql;
    }

    public function parse() : ShowTablesQuery
    {
        if ($this->tokens[$this->pointer]->value !== 'SHOW') {
            throw new SQLFakeParseException("Parser error: expected SHOW");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'TABLES') {
            throw new SQLFakeParseException("Parser error: expected SHOW TABLES");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'LIKE') {
            throw new SQLFakeParseException("Parser error: expected SHOW TABLES LIKE");
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer] ?? null;

        if ($token === null || $token->type !== TokenType::STRING_CONSTANT) {
            throw new SQLFakeParseException("Expected string after LIKE");
        }

        return new ShowTablesQuery($token->value, $this->sql);
    }
}

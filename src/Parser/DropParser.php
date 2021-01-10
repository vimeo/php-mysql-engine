<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\DropTableQuery;

/**
 * Very limited parser for DROP TABLE [IF EXISTS] `table_name`
 */
final class DropParser
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

    public function parse() : DropTableQuery
    {
        if ($this->tokens[$this->pointer]->value !== 'DROP') {
            throw new SQLFakeParseException("Parser error: expected DROP");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'TABLE') {
            throw new SQLFakeParseException("Parser error: expected DROP TABLE");
        }

        $this->pointer++;

        $if_exists = false;

        if ($this->tokens[$this->pointer]->value === 'IF') {
            $this->pointer++;

            if ($this->tokens[$this->pointer]->value !== 'EXISTS') {
                throw new SQLFakeParseException("Parser error: expected IF EXISTS");
            }

            $this->pointer++;

            $if_exists = true;
        }

        $token = $this->tokens[$this->pointer];

        if ($token === null || $token->type !== TokenType::IDENTIFIER) {
            throw new SQLFakeParseException("Expected table name after TRUNCATE");
        }

        return new DropTableQuery($token->value, $if_exists, $this->sql);
    }
}

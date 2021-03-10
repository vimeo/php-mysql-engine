<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\ShowIndexQuery;
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

    /**
     * @return ShowTablesQuery|ShowIndexQuery
     * @throws ParserException
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'SHOW') {
            throw new ParserException("Parser error: expected SHOW");
        }

        $this->pointer++;

        switch ($this->tokens[$this->pointer]->value) {
            case 'TABLES':
                return $this->parseShowTables();
            case 'INDEX':
            case 'INDEXES':
            case 'KEYS':
                return $this->parseShowIndex();
            default:
                throw new ParserException("Parser error: expected SHOW TABLES");
        }

    }

    private function parseShowTables()
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'LIKE') {
            throw new ParserException("Parser error: expected SHOW TABLES LIKE");
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer] ?? null;

        if ($token === null || $token->type !== TokenType::STRING_CONSTANT) {
            throw new ParserException("Expected string after LIKE");
        }

        return new ShowTablesQuery($token->value, $this->sql);
    }

    private function parseShowIndex()
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'FROM') {
            throw new ParserException("Parser error: expected SHOW INDEX FROM");
        }
        $this->pointer++;
        $token = $this->tokens[$this->pointer];
        if ($token === null || $token->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after FROM");
        }
        return new ShowIndexQuery($token->value, $this->sql);
    }
}

<?php

namespace MysqlEngine\Parser;

use MysqlEngine\Query\ShowColumnsQuery;
use MysqlEngine\Query\ShowIndexQuery;
use MysqlEngine\Query\ShowTablesQuery;
use MysqlEngine\TokenType;

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
     * @return ShowColumnsQuery|ShowIndexQuery|ShowTablesQuery
     * @throws ParserException
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'SHOW') {
            throw new ParserException("Parser error: expected SHOW");
        }

        $this->pointer++;
        $isFull = false;
        // For case with TABLES and COLUMNS could be optinaly used argument FULL.
        if ($this->tokens[$this->pointer]->value === 'FULL') {
            $isFull = true;
            $this->pointer++;
        }

        switch ($this->tokens[$this->pointer]->value) {
            case 'TABLES':
                return $this->parseShowTables();
            case 'INDEX':
            case 'INDEXES':
            case 'KEYS':
                return $this->parseShowIndex();
            case 'COLUMNS':
                return $this->parseShowColumns($isFull);
            default:
                throw new ParserException("Parser error: expected SHOW TABLES");
        }
    }

    private function parseShowTables(): ShowTablesQuery
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'LIKE') {
            throw new ParserException("Parser error: expected SHOW [FULL] TABLES LIKE");
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer] ?? null;

        if ($token === null || $token->type !== TokenType::STRING_CONSTANT) {
            throw new ParserException("Expected string after LIKE");
        }

        return new ShowTablesQuery($token->value, $this->sql);
    }

    private function parseShowIndex(): ShowIndexQuery
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'FROM') {
            throw new ParserException("Parser error: expected SHOW INDEX FROM");
        }
        $this->pointer++;
        $token = $this->tokens[$this->pointer];
        if ($token->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after FROM");
        }

        $query = new ShowIndexQuery($token->value, $this->sql);
        $this->pointer++;

        if ($this->pointer < count($this->tokens)) {
            if ($this->tokens[$this->pointer]->value !== 'WHERE') {
                throw new ParserException("Parser error: expected SHOW INDEX FROM [TABLE_NAME] WHERE");
            }
            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
            $query->whereClause = $expression;
        }

        return $query;
    }

    private function parseShowColumns(bool $isFull): ShowColumnsQuery
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'FROM') {
            throw new ParserException("Parser error: expected SHOW [FULL] COLUMNS FROM");
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer];
        if ($token->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after FROM");
        }

        $query = new ShowColumnsQuery($token->value, $this->sql);
        $query->isFull = $isFull;
        $this->pointer++;

        if ($this->pointer < count($this->tokens)) {
            if ($this->tokens[$this->pointer]->value !== 'WHERE') {
                throw new ParserException("Parser error: expected SHOW [FULL] COLUMNS FROM [TABLE_NAME] WHERE");
            }
            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
            [$this->pointer, $expression] = $expression_parser->buildWithPointer();
            $query->whereClause = $expression;
        }

        return $query;
    }
}
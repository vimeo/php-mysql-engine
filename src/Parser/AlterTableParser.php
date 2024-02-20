<?php

namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\AlterTableAutoincrementQuery;
use Vimeo\MysqlEngine\Query\ShowColumnsQuery;
use Vimeo\MysqlEngine\Query\ShowIndexQuery;
use Vimeo\MysqlEngine\Query\ShowTablesQuery;
use Vimeo\MysqlEngine\TokenType;

/**
 * Very limited parser for ALTER TABLE {table} AUTO_INCREMENT=1
 */
final class AlterTableParser
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
     * @return AlterTableAutoincrementQuery
     * @throws ParserException
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'ALTER') {
            throw new ParserException("Parser error: expected ALTER");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== 'TABLE') {
            throw new ParserException("Parser error: expected ALTER TABLE");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after TABLE");
        }
        $table = $this->tokens[$this->pointer]->value;

        $this->pointer++;

        switch ($this->tokens[$this->pointer]->value) {
            case 'AUTO_INCREMENT':
                return $this->parseAlterTableAutoIncrement($table);
        }
    }

    private function parseAlterTableAutoIncrement(string $table): AlterTableAutoincrementQuery
    {
        $this->pointer++;

        if ($this->tokens[$this->pointer]->value !== '=') {
            throw new ParserException("Parser error: expected ALTER TABLE {table} AUTO_INCREMENT=");
        }

        $this->pointer++;

        if ($this->tokens[$this->pointer]->type !== TokenType::NUMERIC_CONSTANT) {
            throw new ParserException("Expected numeric after =");
        }

        $token = $this->tokens[$this->pointer] ?? null;

        return new AlterTableAutoincrementQuery($table, $token->value, $this->sql);
    }
}

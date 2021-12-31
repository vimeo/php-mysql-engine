<?php
namespace MysqlEngine\Parser;

use MysqlEngine\JoinType;
use MysqlEngine\TokenType;
use MysqlEngine\Query\UpdateQuery;


final class UpdateParser
{
    /**
     * @var array<string, int>
     */
    const CLAUSE_ORDER = ['UPDATE' => 1, 'SET' => 2, 'WHERE' => 3, 'ORDER' => 4, 'LIMIT' => 5];

    /**
     * @var string
     */
    private $current_clause = 'UPDATE';

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
     * @return UpdateQuery
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'UPDATE') {
            throw new ParserException("Parser error: expected UPDATE");
        }

        $this->pointer++;
        $count = \count($this->tokens);
        $token = $this->tokens[$this->pointer];

        if ($token === null || $token->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after UPDATE");
        }

        $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
        $query = new UpdateQuery($token->value, $this->sql);
        $this->pointer++;

        while ($this->pointer < $count) {
            $token = $this->tokens[$this->pointer];
            switch ($token->type) {
                case TokenType::CLAUSE:
                    if (\array_key_exists($token->value, self::CLAUSE_ORDER)
                    && self::CLAUSE_ORDER[$this->current_clause] >= self::CLAUSE_ORDER[$token->value]
                    ) {
                        throw new ParserException("Unexpected clause {$token->value}");
                    }

                    $this->current_clause = $token->value;

                    switch ($token->value) {
                        case 'WHERE':
                            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
                            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
                            $query->whereClause = $expression;
                            break;
                        case 'ORDER':
                            $p = new OrderByParser($this->pointer, $this->tokens);
                            list($this->pointer, $query->orderBy) = $p->parse();
                            break;
                        case 'LIMIT':
                            $p = new LimitParser($this->pointer, $this->tokens);
                            list($this->pointer, $query->limitClause) = $p->parse();
                            break;
                        case 'SET':
                            $p = new SetParser($this->pointer, $this->tokens);
                            list($this->pointer, $query->setClause) = $p->parse();
                            break;
                        default:
                            throw new ParserException("Unexpected clause {$token->value}");
                    }
                    break;
                case TokenType::SEPARATOR:
                    if ($token->value !== ';') {
                        throw new ParserException("Unexpected {$token->value}");
                    }
                    break;
                default:
                    throw new ParserException("Unexpected token {$token->value}");
            }
            $this->pointer++;
        }
        return $query;
    }
}

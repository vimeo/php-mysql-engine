<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\DeleteQuery;
use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\TokenType;

final class DeleteParser
{
    /**
     * @var array<string, int>
     */
    const CLAUSE_ORDER = ['DELETE' => 1, 'FROM' => 2, 'WHERE' => 3, 'ORDER' => 4, 'LIMIT' => 5];

    /**
     * @var string
     */
    private $currentClause = 'DELETE';

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
     * @return DeleteQuery
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'DELETE') {
            throw new ParserException("Parser error: expected DELETE");
        }
        $this->pointer++;
        $count = \count($this->tokens);
        $query = new DeleteQuery($this->sql);
        while ($this->pointer < $count) {
            $token = $this->tokens[$this->pointer];

            switch ($token->type) {
                case TokenType::CLAUSE:
                    if (\array_key_exists($token->value, self::CLAUSE_ORDER)
                        && self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token->value]
                    ) {
                        throw new ParserException("Unexpected clause {$token->value}");
                    }

                    $this->currentClause = $token->value;

                    switch ($token->value) {
                        case 'FROM':
                            $this->pointer++;
                            $token = $this->tokens[$this->pointer];
                            if ($token === null || $token->type !== TokenType::IDENTIFIER) {
                                throw new ParserException("Expected table name after FROM");
                            }
                            $table = ['name' => $token->value, 'join_type' => JoinType::JOIN];
                            $query->fromClause = $table;
                            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                            break;
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
                        default:
                            throw new ParserException("Unexpected clause {$token->value}");
                    }
                    break;

                case TokenType::RESERVED:
                case TokenType::IDENTIFIER:
                    if ($this->currentClause === 'DELETE'
                        && ($token->value === 'LOW_PRIORITY'
                            || $token->value === 'QUICK'
                            || $token->value === 'IGNORE')
                    ) {
                        break;
                    }

                    if ($this->currentClause === 'DELETE' && $token->type === TokenType::IDENTIFIER) {
                        $table = ['name' => $token->value, 'join_type' => JoinType::JOIN];
                        $query->fromClause = $table;
                        $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                        $this->currentClause = 'FROM';
                        break;
                    }

                    throw new ParserException("Unexpected token {$token->value}");

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

        if ($query->fromClause === null) {
            throw new ParserException("Expected FROM in DELETE statement");
        }

        return $query;
    }
}

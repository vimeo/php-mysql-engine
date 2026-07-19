<?php

namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\InsertQuery;
use Vimeo\MysqlEngine\TokenType;

final class InsertParser
{
    const CLAUSE_ORDER = ['INSERT' => 1, 'COLUMN_LIST' => 2, 'VALUES' => 3, 'ON' => 4, 'SET' => 5];

    /**
     * @var string
     */
    private $currentClause = 'INSERT';

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
     * @return InsertQuery
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'INSERT') {
            throw new ParserException("Parser error: expected INSERT");
        }

        $this->pointer++;

        $v = $this->tokens[$this->pointer]->value;

        if ($v === 'LOW_PRIORITY' || $v === 'DELAYED' || $v === 'HIGH_PRIORITY') {
            $this->pointer++;
        }

        $ignore_dupes = false;

        if ($this->tokens[$this->pointer]->value === 'IGNORE') {
            $ignore_dupes = true;
            $this->pointer++;
        }

        if ($this->tokens[$this->pointer]->value === 'INTO') {
            $this->pointer++;
        }

        $token = $this->tokens[$this->pointer];

        if ($token === null || $token->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Expected table name after INSERT");
        }

        $this->pointer++;

        $query = new InsertQuery($token->value, $this->sql, $ignore_dupes);
        $count = \count($this->tokens);
        $needs_comma = false;

        while ($this->pointer < $count) {
            $token = $this->tokens[$this->pointer];

            if ($this->currentClause === 'SET' && $token->value === 'VALUES') {
                $token->type = TokenType::SQLFUNCTION;
            }

            switch ($token->type) {
                case TokenType::CLAUSE:
                    if (\array_key_exists($token->value, self::CLAUSE_ORDER)
                    && self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token->value]
                    ) {
                        throw new ParserException("Unexpected clause {$token->value}");
                    }

                    switch ($token->value) {
                        case 'VALUES':
                            do {
                                $this->pointer++;
                                $token = $this->tokens[$this->pointer];

                                if ($token === null || $token->value !== '(') {
                                    throw new ParserException("Expected ( after VALUES");
                                }

                                $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);
                                $values_tokens = \array_slice(
                                    $this->tokens,
                                    $this->pointer + 1,
                                    $close - $this->pointer - 1
                                );
                                $values = $this->parseValues($values_tokens);
                                if (\count($values) !== \count($query->insertColumns)) {
                                    throw new ParserException(
                                        "Insert list contains "
                                        . \count($query->insertColumns)
                                        . ' fields, but values clause contains '
                                        . \count($values)
                                    );
                                }
                                $query->values[] = $values;
                                $this->pointer = $close;
                            } while (($this->tokens[$this->pointer + 1]->value ?? null) === ','
                                && $this->pointer++
                            );

                            break;
                        case 'SET':
                            $p = new SetParser($this->pointer, $this->tokens);
                            list($this->pointer, $query->setClause) = $p->parse();
                            break;

                        case 'SELECT':
                            $select_parser = new SelectParser($this->pointer, $this->tokens, $this->sql);
                            $query->selectQuery = $select_parser->parse();
                            $this->pointer = $select_parser->getPointer();
                            break;

                        default:
                            throw new ParserException("Unexpected clause {$token->value}");
                    }

                    $this->currentClause = $token->value;
                    break;

                case TokenType::IDENTIFIER:
                    if ($needs_comma) {
                        throw new ParserException("Expected , between expressions in INSERT");
                    }

                    if ($this->currentClause !== 'COLUMN_LIST') {
                        throw new ParserException("Unexpected token {$token->value} in INSERT");
                    }

                    $query->insertColumns[] = $token->value;
                    $needs_comma = true;
                    break;

                case TokenType::PAREN:
                    if ($this->currentClause === 'INSERT' && $token->value === '(') {
                        $this->currentClause = 'COLUMN_LIST';
                        break;
                    }

                    throw new ParserException("Unexpected (");

                case TokenType::SEPARATOR:
                    if ($token->value === ',') {
                        if (!$needs_comma) {
                            throw new ParserException("Unexpected ,");
                        }

                        $needs_comma = false;
                    } else {
                        if ($this->currentClause === 'COLUMN_LIST' && $needs_comma && $token->value === ')') {
                            $needs_comma = false;
                            if (!in_array($this->tokens[$this->pointer + 1]->value ?? null, ['VALUES', 'SELECT'])) {
                                throw new ParserException("Expected VALUES after insert column list");
                            }
                            break;
                        }

                        throw new ParserException("Unexpected {$token->value}");
                    }

                    break;

                case TokenType::RESERVED:
                    if ($token->value === 'ON') {
                        $expected = ['DUPLICATE', 'KEY', 'UPDATE'];
                        $next_pointer = $this->pointer + 1;
                        foreach ($expected as $index => $keyword) {
                            $next = $this->tokens[$next_pointer + $index] ?? null;
                            if ($next === null || $next->value !== $keyword) {
                                throw new ParserException("Unexpected keyword near ON");
                            }
                        }
                        $this->pointer += 3;
                        $p = new SetParser($this->pointer, $this->tokens);
                        list($this->pointer, $query->updateExpressions) = $p->parse(true);
                        break;
                    }

                    throw new ParserException("Unexpected {$token->value}");

                default:
                    throw new ParserException("Unexpected token {$token->value}");
            }

            $this->pointer++;
        }
        if ((!$query->insertColumns || !$query->values) && !$query->setClause && !$query->selectQuery) {
            throw new ParserException("Missing values to insert");
        }
        return $query;
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return array<int, Expression>
     */
    protected function parseValues(array $tokens)
    {
        $pointer = 0;
        $count = \count($tokens);
        $expressions = [];
        $needs_comma = false;
        while ($pointer < $count) {
            $token = $tokens[$pointer];
            switch ($token->type) {
                case TokenType::IDENTIFIER:
                case TokenType::NUMERIC_CONSTANT:
                case TokenType::STRING_CONSTANT:
                case TokenType::NULL_CONSTANT:
                case TokenType::OPERATOR:
                case TokenType::SQLFUNCTION:
                case TokenType::PAREN:
                    if ($needs_comma) {
                        throw new ParserException(
                            "Expected , between expressions in SET clause near {$token->value}"
                        );
                    }
                    $expression_parser = new ExpressionParser($tokens, $pointer - 1);

                    list($pointer, $expression) = $expression_parser->buildWithPointer();
                    $expressions[] = $expression;
                    $needs_comma = true;
                    break;
                case TokenType::SEPARATOR:
                    if ($token->value === ',') {
                        if (!$needs_comma) {
                            echo "le comma one";
                            throw new ParserException("Unexpected ,");
                        }
                        $needs_comma = false;
                    } else {
                        throw new ParserException("Unexpected {$token->value}");
                    }
                    break;
                default:
                    throw new ParserException("Unexpected token {$token->value}");
            }
            $pointer++;
        }
        return $expressions;
    }
}

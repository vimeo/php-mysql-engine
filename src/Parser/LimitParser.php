<?php
namespace MysqlEngine\Parser;

use MysqlEngine\TokenType;
use MysqlEngine\Query\Expression\ConstantExpression;
use MysqlEngine\Query\Expression\NamedPlaceholderExpression;
use MysqlEngine\Query\Expression\QuestionMarkPlaceholderExpression;
use MysqlEngine\Query\LimitClause;

final class LimitParser
{
    /**
     * @var int
     */
    private $pointer;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(int $pointer, array $tokens)
    {
        $this->pointer = $pointer;
        $this->tokens = $tokens;
    }

    /**
     * @return array{0:int, 1:LimitClause}
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'LIMIT') {
            throw new ParserException("Parser error: expected LIMIT");
        }
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;

        if ($next === null) {
            throw new ParserException("Expected token after OFFSET");
        }

        if ($next->type === TokenType::NUMERIC_CONSTANT) {
            $limit = new ConstantExpression($next);
        } elseif ($next->type === TokenType::IDENTIFIER && $next->value === '?') {
            if ($next->parameterOffset !== null) {
                $limit = new QuestionMarkPlaceholderExpression($next, $next->parameterOffset);
            } elseif ($next->parameterName !== null) {
                $limit = new NamedPlaceholderExpression($next, $next->parameterName);
            } else {
                throw new ParserException('? encountered with unknown offset');
            }
        } else {
            throw new ParserException("Expected integer or parameter after OFFSET");
        }

        $offset = null;
        $next = $this->tokens[$this->pointer + 1] ?? null;
        if ($next !== null) {
            if ($next->value === 'OFFSET') {
                $this->pointer += 2;
                $next = $this->tokens[$this->pointer] ?? null;

                if ($next === null) {
                    throw new ParserException("Expected token after OFFSET");
                }

                if ($next->type === TokenType::NUMERIC_CONSTANT) {
                    $offset = new ConstantExpression($next);
                } elseif ($next->type === TokenType::IDENTIFIER && $next->value === '?') {
                    $offset = new NamedPlaceholderExpression($next, $next->parameterName);
                } else {
                    throw new ParserException("Expected integer or parameter after OFFSET");
                }
            } else {
                if ($next->value === ',') {
                    $this->pointer += 2;
                    $next = $this->tokens[$this->pointer] ?? null;
                    $offset = $limit;

                    if ($next === null) {
                        throw new ParserException("Expected token after OFFSET");
                    }

                    if ($next->type === TokenType::NUMERIC_CONSTANT) {
                        $limit = new ConstantExpression($next);
                    } elseif ($next->type === TokenType::IDENTIFIER && $next->value === '?') {
                        $limit = new NamedPlaceholderExpression($next, $next->parameterName);
                    } else {
                        throw new ParserException("Expected integer or parameter after OFFSET");
                    }
                }
            }
        }

        return [$this->pointer, new LimitClause($offset, $limit)];
    }
}

<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\Processor\ProcessorException;

final class CaseOperatorExpression extends Expression
{
    /**
     * @var array<int, array{when: Expression, then: Expression}>
     */
    public $whenExpressions = [];

    /**
     * @var Expression|null
     */
    public $case;

    /**
     * @var Expression|null
     */
    public $when;

    /**
     * @var Expression|null
     */
    public $then;

    /**
     * @var Expression|null
     */
    public $else;

    /**
     * @var string
     */
    public $lastKeyword = 'CASE';

    /**
     * @var bool
     */
    public $wellFormed = false;

    public function __construct(Token $token)
    {
        $this->name = 'CASE';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['CASE'];
        $this->operator = 'CASE';
        $this->type = TokenType::OPERATOR;
        $this->start = $token->start;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->wellFormed;
    }

    /**
     * @return void
     */
    public function setKeyword(string $keyword)
    {
        switch ($keyword) {
            case 'WHEN':
                if ($this->lastKeyword !== 'CASE' && $this->lastKeyword !== 'THEN') {
                    throw new ParserException("Unexpected WHEN in CASE statement");
                }
                $this->lastKeyword = 'WHEN';
                $this->when = null;
                $this->then = null;
                break;
            case 'THEN':
                if ($this->lastKeyword !== 'WHEN' || !$this->when) {
                    throw new ParserException("Unexpected THEN in CASE statement");
                }
                $this->lastKeyword = 'THEN';
                break;
            case 'ELSE':
                if ($this->lastKeyword !== 'THEN' || !$this->then) {
                    throw new ParserException("Unexpected ELSE in CASE statement");
                }
                $this->lastKeyword = 'ELSE';
                break;
            case 'END':
                if ($this->lastKeyword === 'THEN' && $this->then) {
                    $this->else = new ConstantExpression(
                        new Token(TokenType::NULL_CONSTANT, 'null', 'null', $this->start)
                    );
                } else {
                    if ($this->lastKeyword !== 'ELSE' || !$this->else) {
                        throw new ParserException("Unexpected END in CASE statement");
                    }
                }
                $this->lastKeyword = 'END';
                $this->wellFormed = true;
                break;
            default:
                throw new ParserException("Unexpected keyword {$keyword} in CASE statement");
        }
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        switch ($this->lastKeyword) {
            case 'CASE':
                if ($this->case && !$overwrite) {
                    throw new ParserException("Unexpected token near CASE");
                }
                $this->case = $expr;
                break;
            case 'WHEN':
                if ($this->when && !$overwrite) {
                    throw new ParserException("Unexpected token near WHEN");
                }
                $this->when = $expr;
                break;
            case 'THEN':
                if ($this->then && !$overwrite) {
                    throw new ParserException("Unexpected token near THEN");
                }
                $this->then = $expr;
                $this->whenExpressions[] = ['when' => ($__tmp1__ = $this->when) !== null ? $__tmp1__ : (function () {
                    throw new \TypeError('Failed assertion');
                })(), 'then' => $expr];
                break;
            case 'ELSE':
                if ($this->else && !$overwrite) {
                    throw new ParserException("Unexpected token near ELSE");
                }
                $this->else = $expr;
                break;
            case 'END':
                throw new ParserException("Unexpected token near END");
        }
    }

    /**
     * @param array<int, Token> $tokens
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        $p = new ExpressionParser($tokens, $pointer, new StubExpression(), 0, true);
        list($pointer, $new_expression) = $p->buildWithPointer();
        if ($negated) {
            $new_expression->negate();
        }
        $this->setNextChild($new_expression, false);
        return $pointer;
    }
}

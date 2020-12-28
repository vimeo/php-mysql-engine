<?php
namespace Vimeo\MysqlEngine\Query\Expression;


use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;

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

    /**
     * @param array{type: TokenType::*, value: string, raw: string} $_token
     */
    public function __construct(array $_token)
    {
        $this->name = 'CASE';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['CASE'];
        $this->operator = 'CASE';
        $this->type = TokenType::OPERATOR;
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
                    throw new SQLFakeParseException("Unexpected WHEN in CASE statement");
                }
                $this->lastKeyword = 'WHEN';
                $this->when = null;
                $this->then = null;
                break;
            case 'THEN':
                if ($this->lastKeyword !== 'WHEN' || !$this->when) {
                    throw new SQLFakeParseException("Unexpected THEN in CASE statement");
                }
                $this->lastKeyword = 'THEN';
                break;
            case 'ELSE':
                if ($this->lastKeyword !== 'THEN' || !$this->then) {
                    throw new SQLFakeParseException("Unexpected ELSE in CASE statement");
                }
                $this->lastKeyword = 'ELSE';
                break;
            case 'END':
                if ($this->lastKeyword === 'THEN' && $this->then) {
                    $this->else = new ConstantExpression(['type' => TokenType::NULL_CONSTANT, 'value' => 'null', 'raw' => 'null']);
                } else {
                    if ($this->lastKeyword !== 'ELSE' || !$this->else) {
                        throw new SQLFakeParseException("Unexpected END in CASE statement");
                    }
                }
                $this->lastKeyword = 'END';
                $this->wellFormed = true;
                break;
        default:
            throw new SQLFakeParseException("Unexpected keyword {$keyword} in CASE statement");
        }
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        switch ($this->lastKeyword) {
            case 'CASE':
                if ($this->case && !$overwrite) {
                    throw new SQLFakeParseException("Unexpected token near CASE");
                }
                $this->case = $expr;
                break;
            case 'WHEN':
                if ($this->when && !$overwrite) {
                    throw new SQLFakeParseException("Unexpected token near WHEN");
                }
                $this->when = $expr;
                break;
            case 'THEN':
                if ($this->then && !$overwrite) {
                    throw new SQLFakeParseException("Unexpected token near THEN");
                }
                $this->then = $expr;
                $this->whenExpressions[] = ['when' => ($__tmp1__ = $this->when) !== null ? $__tmp1__ : (function () {
                    throw new \TypeError('Failed assertion');
                })(), 'then' => $expr];
                break;
            case 'ELSE':
                if ($this->else && !$overwrite) {
                    throw new SQLFakeParseException("Unexpected token near ELSE");
                }
                $this->else = $expr;
                break;
            case 'END':
                throw new SQLFakeParseException("Unexpected token near END");
        }
    }

    /**
     * @param array<int, array{type: TokenType::*, value: string, raw: string}> $tokens
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        $p = new ExpressionParser($tokens, $pointer, new PlaceholderExpression(), 0, true);
        list($pointer, $new_expression) = $p->buildWithPointer();
        if ($negated) {
            $new_expression->negate();
        }
        $this->setNextChild($new_expression, false);
        return $pointer;
    }
}

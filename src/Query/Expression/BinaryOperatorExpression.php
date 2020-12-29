<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;

final class BinaryOperatorExpression extends Expression
{
    /**
     * @var bool
     */
    protected $evaluates_groups = false;

    /**
     * @var int
     */
    public $negatedInt = 0;

    /**
     * @var Expression
     */
    public $left;

    /**
     * @var bool
     */
    public $negated = false;

    /**
     * @var string
     */
    public $operator = '';

    /**
     * @var Expression|null
     */
    public $right = null;

    public function __construct(
        Expression $left,
        bool $negated = false,
        string $operator = '',
        ?Expression $right = null
    ) {
        $this->left = $left;
        $this->negated = $negated;
        $this->operator = $operator;
        $this->right = $right;
        $this->name = '';
        $this->precedence = 0;
        $this->type = TokenType::OPERATOR;
        if (!($operator === null || $operator === '')) {
            $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
        }
        $this->negatedInt = $this->negated ? 1 : 0;
    }

    /**
     * @return void
     */
    public function negate()
    {
        $this->negated = true;
        $this->negatedInt = 1;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->right && !($this->operator === null || $this->operator === '');
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        if (($this->operator === null || $this->operator === '') || $this->right && !$overwrite) {
            throw new SQLFakeParseException("Parse error");
        }
        $this->right = $expr;
    }

    /**
     * @return void
     */
    public function setOperator(string $operator)
    {
        $this->operator = $operator;
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
    }

    /**
     * @return Expression
     */
    public function getRightOrThrow()
    {
        if ($this->right === null) {
            throw new SQLFakeParseException("Parse error: attempted to resolve unbound expression");
        }
        return $this->right;
    }

    /**
     * @return array<int, Expression>
     */
    public function traverse()
    {
        $container = [];
        if ($this->left !== null) {
            if ($this->left instanceof BinaryOperatorExpression) {
                $container = \array_merge($this->left->traverse(), $container);
            } else {
                $container[] = $this->left;
            }
        }
        if ($this->right !== null) {
            if ($this->right instanceof BinaryOperatorExpression) {
                $container = \array_merge($this->right->traverse(), $container);
            } else {
                $container[] = $this->right;
            }
        }
        return $container;
    }

    /**
     * @param array<int, array{type: TokenType::*, value: string, raw: string}> $tokens
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        $tmp = $this->right ? new BinaryOperatorExpression($this->right) : new PlaceholderExpression();
        $p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, true);
        list($pointer, $new_expression) = $p->buildWithPointer();
        if ($negated) {
            $new_expression->negate();
        }
        $this->setNextChild($new_expression, true);
        return $pointer;
    }
}

<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\Processor\ProcessorException;

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
        if ($operator !== '' && \substr($operator, 0, 4) === 'NOT ') {
            $operator = \substr($operator, 4);
            $negated = !$negated;
        }

        $this->left = $left;
        $this->negated = $negated;
        $this->operator = $operator;
        $this->right = $right;
        $this->name = '';
        $this->precedence = 0;
        $this->type = TokenType::OPERATOR;
        if ($operator !== '') {
            $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
        }
        $this->negatedInt = $this->negated ? 1 : 0;
        $this->start = $left->start;
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
        return $this->right && $this->operator;
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        if (!$this->operator || ($this->right && !$overwrite)) {
            throw new ParserException("Parse error");
        }
        $this->right = $expr;
    }

    /**
     * @return void
     */
    public function setOperator(string $operator)
    {
        if (\substr($operator, 0, 4) === 'NOT ') {
            $operator = \substr($operator, 4);
            $this->negatedInt = 1 - $this->negatedInt;
        }

        $this->operator = $operator;
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
    }

    /**
     * @return Expression
     */
    public function getRightOrThrow()
    {
        if ($this->right === null) {
            throw new ParserException("Parse error: attempted to resolve unbound expression");
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
     * @param array<int, Token> $tokens
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        $tmp = $this->right ? new BinaryOperatorExpression($this->right) : new StubExpression();
        $p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, true);
        list($pointer, $new_expression) = $p->buildWithPointer();
        if ($negated) {
            $new_expression->negate();
        }
        $this->setNextChild($new_expression, true);
        return $pointer;
    }

    public function hasAggregate() : bool
    {
        return $this->left->hasAggregate() || $this->right->hasAggregate();
    }
}

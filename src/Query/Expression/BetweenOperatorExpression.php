<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\ExpressionParser;
use MysqlEngine\Parser\Token;
use MysqlEngine\TokenType;
use MysqlEngine\Parser\ParserException;
use MysqlEngine\Processor\ProcessorException;

final class BetweenOperatorExpression extends Expression
{
    /**
     * @var Expression|null
     */
    public $beginning = null;

    /**
     * @var Expression|null
     */
    public $end = null;

    /**
     * @var bool
     */
    public $and = false;

    /**
     * @var bool
     */
    protected $evaluates_groups = false;

    /**
     * @var bool
     */
    public $negated = false;

    /**
     * @var Expression
     */
    public $left;

    public function __construct(Expression $left)
    {
        $this->left = $left;
        $this->name = '';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['BETWEEN'];
        $this->operator = 'BETWEEN';
        $this->type = TokenType::OPERATOR;
        $this->start = $left->start;
    }

    /**
     * @return void
     */
    public function negate()
    {
        $this->negated = true;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->beginning && $this->end;
    }

    /**
     * @return void
     */
    public function setStart(Expression $expr)
    {
        $this->beginning = $expr;
    }

    /**
     * @return void
     */
    public function setEnd(Expression $expr)
    {
        $this->end = $expr;
    }

    /**
     * @return void
     */
    public function foundAnd()
    {
        if ($this->and || !$this->beginning) {
            throw new ParserException("Unexpected AND");
        }
        $this->and = true;
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        if ($overwrite) {
            if ($this->end) {
                $this->end = $expr;
            } else {
                if ($this->beginning) {
                    $this->beginning = $expr;
                } else {
                    $this->left = $expr;
                }
            }
            return;
        }
        if (!$this->beginning) {
            $this->beginning = $expr;
        } else {
            if ($this->and && !$this->end) {
                $this->end = $expr;
            } else {
                throw new ParserException("Parse error: unexpected token in BETWEEN statement");
            }
        }
    }

    /**
     * @return Expression
     */
    private function getLatestExpression()
    {
        if ($this->end) {
            return $this->end;
        }
        if ($this->beginning) {
            return $this->beginning;
        }
        return $this->left;
    }

    /**
     * @param array<int, Token> $tokens
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        $tmp = new BinaryOperatorExpression($this->getLatestExpression());
        $p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, true);
        list($pointer, $new_expression) = $p->buildWithPointer();
        if ($negated) {
            $new_expression->negate();
        }
        $this->setNextChild($new_expression, true);
        return $pointer;
    }
}

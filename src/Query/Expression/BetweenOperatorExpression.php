<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\Processor\ProcessorException;

final class BetweenOperatorExpression extends Expression
{
    /**
     * @var Expression|null
     */
    public $start = null;

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
        return $this->start && $this->end;
    }

    /**
     * @return void
     */
    public function setStart(Expression $expr)
    {
        $this->start = $expr;
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
        if ($this->and || !$this->start) {
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
                if ($this->start) {
                    $this->start = $expr;
                } else {
                    $this->left = $expr;
                }
            }
            return;
        }
        if (!$this->start) {
            $this->start = $expr;
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
        if ($this->start) {
            return $this->start;
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

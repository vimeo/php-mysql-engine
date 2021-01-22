<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\TokenType;

final class IntervalOperatorExpression extends Expression
{
    private const UNIT_VALUES = [
        'DAY' => true,
        'DAY_HOUR' => true,
        'DAY_MICROSECOND' => true,
        'DAY_MINUTE' => true,
        'DAY_SECOND' => true,
        'HOUR' => true,
        'HOUR_MICROSECOND' => true,
        'HOUR_MINUTE' => true,
        'HOUR_SECOND' => true,
        'MICROSECOND' => true,
        'MINUTE' => true,
        'MINUTE_MICROSECOND' => true,
        'MINUTE_SECOND' => true,
        'MONTH' => true,
        'QUARTER' => true,
        'SECOND' => true,
        'SECOND_MICROSECOND' => true,
        'WEEK' => true,
        'YEAR' => true,
        'YEAR_MONTH' => true,
    ];

    /** @var ?Expression */
    public $number = null;

    /** @var ?string */
    public $unit = null;

    public function __construct()
    {
        $this->name = '';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['INTERVAL'];
        $this->operator = 'INTERVAL';
        $this->type = TokenType::OPERATOR;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->number !== null && $this->unit !== null;
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        $this->number = $expr;
    }

    public function setUnit(string $unit)
    {
        $unit = \strtoupper($unit);

        if (!\array_key_exists($unit, self::UNIT_VALUES)) {
            throw new ParserException("Unexpected unit");
        }

        $this->unit = $unit;
    }
}

<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\PositionExpression;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class PositionEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(PositionExpression $expr, array $row)
    {
        $first_row = reset($row);

        if (\is_array($first_row)) {
            $row = $first_row;
        }

        if (!\array_key_exists($expr->position - 1, $row)) {
            throw new ProcessorException(
                "Undefined positional reference {$expr->position} IN GROUP BY or ORDER BY"
            );
        }

        return $row[$expr->position - 1];
    }
}

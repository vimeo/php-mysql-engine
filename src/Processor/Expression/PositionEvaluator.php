<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\PositionExpression;

final class PositionEvaluator
{
    /**
     * @param array<string, mixed> $data
     *
     * @return mixed
     */
    public static function evaluate(PositionExpression $expr, array $data, \Vimeo\MysqlEngine\FakePdo $_conn)
    {
        $first_row = reset($data);

        if (\is_array($first_row)) {
            $row = $first_row;
        }

        $row = (array) $row;
        if (!\array_key_exists($expr->position - 1, $row)) {
            throw new SQLFakeRuntimeException(
                "Undefined positional reference {$expr->position} IN GROUP BY or ORDER BY"
            );
        }

        return $row[$expr->position - 1];
    }
}

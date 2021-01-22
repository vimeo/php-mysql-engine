<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\PositionExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;

final class PositionEvaluator
{
    /**
     * @param array<string, mixed> $data
     *
     * @return mixed
     */
    public static function evaluate(PositionExpression $expr, array $data)
    {
        $first_row = reset($data);

        if (\is_array($first_row)) {
            $row = $first_row;
        }

        $row = (array) $row;
        if (!\array_key_exists($expr->position - 1, $row)) {
            throw new ProcessorException(
                "Undefined positional reference {$expr->position} IN GROUP BY or ORDER BY"
            );
        }

        return $row[$expr->position - 1];
    }
}

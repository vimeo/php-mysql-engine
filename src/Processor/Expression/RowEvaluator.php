<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\RowExpression;

final class RowEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(RowExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $result = [];
        foreach ($expr->elements as $expr) {
            $result[] = Evaluator::evaluate($expr, $row, $conn);
        }
        return $result;
    }
}

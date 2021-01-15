<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;

final class RowEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        RowExpression $expr,
        array $row,
        array $columns
    ) {
        $result = [];
        foreach ($expr->elements as $expr) {
            $result[] = Evaluator::evaluate($conn, $scope, $expr, $row, $columns);
        }
        return $result;
    }
}

<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Query\Expression\RowExpression;
use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class RowEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        RowExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $rows = [];
        foreach ($expr->elements as $expr) {
            $rows[] = Evaluator::evaluate($conn, $scope, $expr, $row, $result);
        }
        return $rows;
    }
}

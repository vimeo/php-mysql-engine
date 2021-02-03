<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Processor\QueryResult;
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
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
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

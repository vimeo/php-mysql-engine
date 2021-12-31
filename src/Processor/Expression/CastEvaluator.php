<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\CastExpression;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class CastEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        CastExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $val = Evaluator::evaluate($conn, $scope, $expr->expr, $row, $result);

        // TODO: more stuff

        return $val;
    }
}

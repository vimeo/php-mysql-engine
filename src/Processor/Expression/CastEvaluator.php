<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\CastExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;

final class CastEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
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

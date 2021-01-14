<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\CastExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class CastEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, CastExpression $expr, array $row)
    {
        $val = Evaluator::evaluate($conn, $scope, $expr->expr, $row);

        // TODO: more stuff

        return $val;
    }
}

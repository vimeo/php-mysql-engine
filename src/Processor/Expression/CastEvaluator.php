<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\CastExpression;

final class CastEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(CastExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $val = Evaluator::evaluate($expr->expr, $row, $conn);

        // TODO: more stuff

        return $val;
    }
}

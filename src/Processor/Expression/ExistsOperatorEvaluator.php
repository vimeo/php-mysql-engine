<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class ExistsOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, ExistsOperatorExpression $expr, array $row)
    {
        if (!$expr->isWellFormed()) {
            throw new SQLFakeParseException("Parse error: empty EXISTS subquery");
        }

        $ret = Evaluator::evaluate($conn, $scope, $expr->exists, $row);

        if ($expr->negated) {
            return $ret ? 0 : 1;
        }

        return $ret ? 1 : 0;
    }
}

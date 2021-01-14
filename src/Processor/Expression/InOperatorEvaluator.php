<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Query\Expression\InOperatorExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class InOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, InOperatorExpression $expr, array $row)
    {
        $inList = $expr->inList;

        if ($inList === null || \count($inList) === 0) {
            throw new SQLFakeParseException("Parse error: empty IN list");
        }

        if (\count($inList) === 1 && Evaluator::evaluate($conn, $scope, $inList[0], $row) === null) {
            if (!$expr->negated) {
                return false;
            }

            throw new SQLFakeRuntimeException(
                "You're probably trying to use NOT IN with an empty array, but MySQL would evaluate this to false."
            );
        }

        $value = Evaluator::evaluate($conn, $scope, $expr->left, $row);

        foreach ($inList as $in_expr) {
            if ($in_expr instanceof SubqueryExpression) {
                $ret = Evaluator::evaluate($conn, $scope, $in_expr, $row);

                foreach ($ret as $r) {
                    if (\count($r) !== 1) {
                        throw new SQLFakeRuntimeException("Subquery result should contain 1 column");
                    }

                    foreach ($r as $val) {
                        if ($value == $val) {
                            return !$expr->negated;
                        }
                    }
                }
            } else {
                if ($value == Evaluator::evaluate($conn, $scope, $in_expr, $row)) {
                    return !$expr->negated;
                }
            }
        }

        return $expr->negated;
    }
}

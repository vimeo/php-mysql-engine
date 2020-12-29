<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Query\Expression\InOperatorExpression;

final class InOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(InOperatorExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $inList = $expr->inList;

        if ($inList === null || \count($inList) === 0) {
            throw new SQLFakeParseException("Parse error: empty IN list");
        }

        if (\count($inList) === 1 && Evaluator::evaluate($inList[0], $row, $conn) === null) {
            if (!$expr->negated) {
                return false;
            }

            throw new SQLFakeRuntimeException(
                "You're probably trying to use NOT IN with an empty array, but MySQL would evaluate this to false."
            );
        }

        $value = Evaluator::evaluate($expr->left, $row, $conn);

        foreach ($inList as $in_expr) {
            if ($in_expr instanceof SubqueryExpression) {
                $ret = Evaluator::evaluate($in_expr, $row, $conn);

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
                if ($value == Evaluator::evaluate($in_expr, $row, $conn)) {
                    return !$expr->negated;
                }
            }
        }

        return $expr->negated;
    }
}

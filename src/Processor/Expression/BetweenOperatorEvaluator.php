<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression;

final class BetweenOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        BetweenOperatorExpression $expr,
        array $row,
        array $columns
    ) : bool {
        $start = $expr->start;
        $end = $expr->end;

        if ($start === null || $end === null) {
            throw new SQLFakeRuntimeException("Attempted to evaluate incomplete BETWEEN expression");
        }

        $subject = Evaluator::evaluate($conn, $scope, $expr->left, $row, $columns);
        $start = Evaluator::evaluate($conn, $scope, $start, $row, $columns);
        $end = Evaluator::evaluate($conn, $scope, $end, $row, $columns);
        if (\is_int($__tmp__ = $subject) || \is_float($__tmp__)) {
            $subject = (int) $subject;
            $start = (int) $start;
            $end = (int) $end;
            $eval = $subject >= $start && $subject <= $end;
        } else {
            $subject = (string) $subject;
            $start = (string) $start;
            $end = (string) $end;
            $eval = $subject >= $start && $subject <= $end;
        }
        return $expr->negated ? !$eval : !!$eval;
    }
}

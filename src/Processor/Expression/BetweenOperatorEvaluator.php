<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression;

final class BetweenOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     */
    public static function evaluate(
        BetweenOperatorExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn) : bool
    {
        $start = $expr->start;
        $end = $expr->end;

        if ($start === null || $end === null) {
            throw new SQLFakeRuntimeException("Attempted to evaluate incomplete BETWEEN expression");
        }

        $subject = Evaluator::evaluate($expr->left, $row, $conn);
        $start = Evaluator::evaluate($start, $row, $conn);
        $end = Evaluator::evaluate($end, $row, $conn);
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

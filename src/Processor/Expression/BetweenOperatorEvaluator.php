<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression;

final class BetweenOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        BetweenOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) : bool {
        $beginning = $expr->beginning;
        $end = $expr->end;

        if ($beginning === null || $end === null) {
            throw new ProcessorException("Attempted to evaluate incomplete BETWEEN expression");
        }

        $subject = Evaluator::evaluate($conn, $scope, $expr->left, $row, $result);
        $beginning = Evaluator::evaluate($conn, $scope, $beginning, $row, $result);
        $end = Evaluator::evaluate($conn, $scope, $end, $row, $result);
        if (\is_int($__tmp__ = $subject) || \is_float($__tmp__)) {
            $subject = (int) $subject;
            $beginning = (int) $beginning;
            $end = (int) $end;
            $eval = $subject >= $beginning && $subject <= $end;
        } else {
            $subject = (string) $subject;
            $beginning = (string) $beginning;
            $end = (string) $end;
            $eval = $subject >= $beginning && $subject <= $end;
        }
        return $expr->negated ? !$eval : !!$eval;
    }
}

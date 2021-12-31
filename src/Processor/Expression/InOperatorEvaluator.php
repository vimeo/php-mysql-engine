<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Parser\ParserException;
use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\SubqueryExpression;
use MysqlEngine\Query\Expression\InOperatorExpression;
use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class InOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        InOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $inList = $expr->inList;

        if ($inList === null || \count($inList) === 0) {
            throw new ParserException("Parse error: empty IN list");
        }

        $value = Evaluator::evaluate($conn, $scope, $expr->left, $row, $result);

        if ($value === null) {
            return $expr->negated;
        }

        foreach ($inList as $in_expr) {
            if ($in_expr instanceof SubqueryExpression) {
                $subquery_result = \MysqlEngine\Processor\SelectProcessor::process(
                    $conn,
                    $scope,
                    $in_expr->query,
                    $row,
                    $result->columns
                );

                foreach ($subquery_result->rows as $r) {
                    if (\count($r) !== 1) {
                        throw new ProcessorException("Subquery result should contain 1 column");
                    }

                    foreach ($r as $val) {
                        if ($value == $val) {
                            return !$expr->negated;
                        }
                    }
                }
            } else {
                if ($value == Evaluator::evaluate($conn, $scope, $in_expr, $row, $result)) {
                    return !$expr->negated;
                }
            }
        }

        return $expr->negated;
    }
}

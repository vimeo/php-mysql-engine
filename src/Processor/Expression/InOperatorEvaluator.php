<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Query\Expression\InOperatorExpression;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;

final class InOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
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
                $subquery_result = \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
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

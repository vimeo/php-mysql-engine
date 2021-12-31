<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\CaseOperatorExpression;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class CaseOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        CaseOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if (!$expr->wellFormed) {
            throw new ProcessorException("Attempted to evaluate incomplete CASE expression");
        }

        foreach ($expr->whenExpressions as $clause) {
            $when = Evaluator::evaluate($conn, $scope, $clause['when'], $row, $result);

            if ($expr->case) {
                $evaluated_case = Evaluator::evaluate($conn, $scope, $expr->case, $row, $result);

                if ($evaluated_case == $when) {
                    return Evaluator::evaluate($conn, $scope, $clause['then'], $row, $result);
                }
            } elseif ((bool) $when) {
                return Evaluator::evaluate($conn, $scope, $clause['then'], $row, $result);
            }
        }

        \assert($expr->else !== null, 'must have else since wellFormed was true');
        return Evaluator::evaluate($conn, $scope, $expr->else, $row, $result);
    }
}

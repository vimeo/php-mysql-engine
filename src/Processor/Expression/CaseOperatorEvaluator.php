<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class CaseOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, CaseOperatorExpression $expr, array $row)
    {
        if (!$expr->wellFormed) {
            throw new SQLFakeRuntimeException("Attempted to evaluate incomplete CASE expression");
        }

        foreach ($expr->whenExpressions as $clause) {
            $when = Evaluator::evaluate($conn, $scope, $clause['when'], $row);

            if ($expr->case) {
                $evaluated_case = Evaluator::evaluate($conn, $scope, $expr->case, $row);

                if ($evaluated_case == $when) {
                    return Evaluator::evaluate($conn, $scope, $clause['then'], $row);
                }
            } elseif ((bool) $when) {
                return Evaluator::evaluate($conn, $scope, $clause['then'], $row);
            }
        }

        \assert($expr->else !== null, 'must have else since wellFormed was true');
        return Evaluator::evaluate($conn, $scope, $expr->else, $row);
    }
}

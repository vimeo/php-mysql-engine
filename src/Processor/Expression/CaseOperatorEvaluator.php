<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;

final class CaseOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(CaseOperatorExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        if (!$expr->wellFormed) {
            throw new SQLFakeRuntimeException("Attempted to evaluate incomplete CASE expression");
        }

        foreach ($expr->whenExpressions as $clause) {
            $when = Evaluator::evaluate($clause['when'], $row, $conn);

            if ($expr->case) {
                $evaluated_case = Evaluator::evaluate($expr->case, $row, $conn);

                if ($evaluated_case == $when) {
                    return Evaluator::evaluate($clause['then'], $row, $conn);
                }
            } elseif ((bool) $when) {
                return Evaluator::evaluate($clause['then'], $row, $conn);
            }
        }

        \assert($expr->else !== null, 'must have else since wellFormed was true');
        return Evaluator::evaluate($expr->else, $row, $conn);
    }
}

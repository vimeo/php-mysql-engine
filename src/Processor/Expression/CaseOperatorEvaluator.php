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

        if ($expr->case) {
            $case_row = Evaluator::evaluate($expr->case, $row, $conn);
            $row = array_merge($row, [$expr->case->name => $case_row]);
        }

        foreach ($expr->whenExpressions as $clause) {
            if ((bool) Evaluator::evaluate($clause['when'], $row, $conn)) {
                return Evaluator::evaluate($clause['then'], $row, $conn);
            }
        }

        \assert($expr->else !== null, 'must have else since wellFormed was true');
        return Evaluator::evaluate($expr->else, $row, $conn);
    }
}

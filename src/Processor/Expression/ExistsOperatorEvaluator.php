<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression;

final class ExistsOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(ExistsOperatorExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        if (!$expr->isWellFormed()) {
            throw new SQLFakeParseException("Parse error: empty EXISTS subquery");
        }

        $ret = Evaluator::evaluate($expr->exists, $row, $conn);

        if ($expr->negated) {
            return $ret ? 0 : 1;
        }

        return $ret ? 1 : 0;
    }
}

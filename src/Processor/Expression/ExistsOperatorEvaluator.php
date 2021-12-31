<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Parser\ParserException;
use MysqlEngine\Query\Expression\ExistsOperatorExpression;
use MysqlEngine\Query\Expression\SubqueryExpression;
use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class ExistsOperatorEvaluator
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
        ExistsOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if (!$expr->isWellFormed()) {
            throw new ParserException("Parse error: empty EXISTS subquery");
        }

        if ($expr->exists instanceof SubqueryExpression) {
            $ret = \MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                $scope,
                $expr->exists->query,
                $row,
                $result->columns
            )->rows;
        } else {
            $ret = Evaluator::evaluate($conn, $scope, $expr->exists, $row, $result);
        }

        if ($expr->negated) {
            return $ret ? 0 : 1;
        }

        return $ret ? 1 : 0;
    }
}

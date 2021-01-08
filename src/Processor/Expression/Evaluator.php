<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Expression;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;

class Evaluator
{
    /**
     * @param  array<string, mixed> $row
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\Query\Expression\Expression $expr,
        array $row,
        \Vimeo\MysqlEngine\FakePdo $conn
    ) {
        switch (get_class($expr)) {
            case \Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression::class:
                return BetweenOperatorEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression::class:
                return BinaryOperatorEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression::class:
                return CaseOperatorEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\ColumnExpression::class:
                return ColumnEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\ConstantExpression::class:
                return $expr->value;

            case \Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression::class:
                return ExistsOperatorEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\FunctionExpression::class:
                return FunctionEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\InOperatorExpression::class:
                return InOperatorEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\PlaceholderExpression::class:
                throw new SQLFakeRuntimeException("Attempted to evaluate placeholder expression!");

            case \Vimeo\MysqlEngine\Query\Expression\PositionExpression::class:
                return PositionEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\RowExpression::class:
                return RowEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\SubqueryExpression::class:
                $evaluated = \Vimeo\MysqlEngine\Processor\SelectProcessor::process($conn, $expr->query, $row);

                $has_aggregate = \count($expr->query->selectExpressions) === 1
                    && \count($evaluated) === 1
                    && $expr->query->selectExpressions[0]->hasAggregate();

                if ($has_aggregate) {
                    return reset($evaluated[0]);
                }

                return $evaluated;

            case \Vimeo\MysqlEngine\Query\Expression\UnaryExpression::class:
                return UnaryEvaluator::evaluate($expr, $row, $conn);

            case \Vimeo\MysqlEngine\Query\Expression\CastExpression::class:
                return CastEvaluator::evaluate($expr, $row, $conn);

            default:
                throw new SQLFakeRuntimeException('Unsupported expression ' . get_class($expr));
        }
    }
}

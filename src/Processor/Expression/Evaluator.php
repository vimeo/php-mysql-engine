<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Expression;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;

class Evaluator
{
    /**
     * @param  array<string, mixed> $row
     * @param array<string, Column> $columns
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        \Vimeo\MysqlEngine\Query\Expression\Expression $expr,
        array $row,
        array $columns
    ) {
        switch (get_class($expr)) {
            case \Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression::class:
                return BetweenOperatorEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression::class:
                return BinaryOperatorEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression::class:
                return CaseOperatorEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\ColumnExpression::class:
                return ColumnEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\ConstantExpression::class:
                return $expr->value;

            case \Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression::class:
                return ExistsOperatorEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\FunctionExpression::class:
                return FunctionEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\InOperatorExpression::class:
                return InOperatorEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\PlaceholderExpression::class:
                throw new SQLFakeRuntimeException("Attempted to evaluate placeholder expression!");

            case \Vimeo\MysqlEngine\Query\Expression\PositionExpression::class:
                return PositionEvaluator::evaluate($expr, $row);

            case \Vimeo\MysqlEngine\Query\Expression\RowExpression::class:
                return RowEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\SubqueryExpression::class:
                [$evaluated] = \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
                    $conn,
                    $scope,
                    $expr->query,
                    $row,
                    $columns
                );

                $has_aggregate = \count($expr->query->selectExpressions) === 1
                    && \count($evaluated) === 1
                    && $expr->query->selectExpressions[0]->hasAggregate();

                if ($has_aggregate) {
                    return reset($evaluated[0]);
                }

                return $evaluated;

            case \Vimeo\MysqlEngine\Query\Expression\UnaryExpression::class:
                return UnaryEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\CastExpression::class:
                return CastEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\VariableExpression::class:
                return VariableEvaluator::evaluate($conn, $scope, $expr, $row, $columns);

            default:
                throw new SQLFakeRuntimeException('Unsupported expression ' . get_class($expr));
        }
    }

    /**
     * @param  array<string, Column> $columns
     * @return Column
     */
    public static function getColumnSchema(
        \Vimeo\MysqlEngine\Query\Expression\Expression $expr,
        Scope $scope,
        array $columns
    ) : Column {
        switch (get_class($expr)) {
            case \Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression::class:
                return new Column\TinyInt(true, 1);

            case \Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression::class:
                return BinaryOperatorEvaluator::getColumnSchema($expr, $scope, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression::class:
                // TODO
                break;

            case \Vimeo\MysqlEngine\Query\Expression\ColumnExpression::class:
                return ColumnEvaluator::getColumnSchema($expr, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\ConstantExpression::class:
                switch ($expr->getType()) {
                    case TokenType::NUMERIC_CONSTANT:
                        if (\strpos($expr->value, '.') !== false) {
                            return new Column\FloatColumn(10, 2);
                        }

                        return new Column\IntColumn(false, 10);

                    case TokenType::STRING_CONSTANT:
                        return new Column\Varchar(10);

                    case TokenType::NULL_CONSTANT:
                        return new Column\Varchar(10);
                }
                break;

            case \Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression::class:
                return new Column\TinyInt(true, 1);

            case \Vimeo\MysqlEngine\Query\Expression\FunctionExpression::class:
                return FunctionEvaluator::getColumnSchema($expr, $scope, $columns);

            case \Vimeo\MysqlEngine\Query\Expression\InOperatorExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\PlaceholderExpression::class:
                throw new SQLFakeRuntimeException("Attempted to evaluate placeholder expression!");

            case \Vimeo\MysqlEngine\Query\Expression\PositionExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\RowExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\SubqueryExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\UnaryExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\CastExpression::class:
                break;

            case \Vimeo\MysqlEngine\Query\Expression\VariableExpression::class:
                if (array_key_exists($expr->variableName, $scope->variables)) {
                    $value = $scope->variables[$expr->variableName];

                    if (\is_int($value)) {
                        return new Column\IntColumn(false, 10);
                    }
                    
                    if (\is_float($value)) {
                        return new Column\FloatColumn(10, 2);
                    }
                }

                break;
        }

        return new Column\Varchar(10);
    }
}

<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\Processor\Expression\Evaluator as ExpressionEvaluator;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;

final class JoinProcessor
{
    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $left_dataset
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $right_dataset
     * @param JoinType::*                      $join_type
     * @param 'USING'|'OM'|null                $_ref_type
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    public static function process(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        array $left_dataset,
        array $right_dataset,
        string $right_table_name,
        $join_type,
        $_ref_type,
        ?Expression $filter
    ) {
        $rows = [];

        switch ($join_type) {
            case JoinType::JOIN:
            case JoinType::STRAIGHT:
                $joined_columns = array_merge($left_dataset[1], $right_dataset[1]);

                foreach ($left_dataset[0] as $row) {
                    foreach ($right_dataset[0] as $r) {
                        $left_row = $row;
                        $candidate_row = \array_merge($row, $r);
                        if (!$filter
                            || ExpressionEvaluator::evaluate($conn, $scope, $filter, $candidate_row, $joined_columns)
                        ) {
                            $rows[] = $candidate_row;
                        }
                    }
                }
                break;

            case JoinType::LEFT:
                $null_placeholder = [];

                foreach ($right_dataset[1] as $name => $column) {
                    $parts = explode('.%.', $name);
                    $null_placeholder[$right_table_name . '.%.' . end($parts)] = null;
                    $column = clone $column;
                    $column->isNullable = true;
                    $right_dataset[1][$name] = $column;
                }

                $joined_columns = array_merge($left_dataset[1], $right_dataset[1]);

                foreach ($left_dataset[0] as $left_row) {
                    $any_match = false;
                    foreach ($right_dataset[0] as $right_row) {
                        $candidate_row = \array_merge($left_row, $right_row);

                        if (!$filter
                            || ExpressionEvaluator::evaluate($conn, $scope, $filter, $candidate_row, $joined_columns)
                        ) {
                            $rows[] = $candidate_row;
                            $any_match = true;
                        }
                    }
                    if (!$any_match) {
                        $rows[] = \array_merge($left_row, $null_placeholder);
                    }
                }

                break;

            case JoinType::RIGHT:
                $null_placeholder = [];

                foreach ($right_dataset[1] as $name => $_) {
                    $parts = explode('.%.', $name);
                    $null_placeholder[$right_table_name . '.%.' . end($parts)] = null;
                }

                $joined_columns = array_merge($left_dataset[1], $right_dataset[1]);

                foreach ($right_dataset[0] as $raw) {
                    $any_match = false;

                    foreach ($left_dataset[0] as $row) {
                        $left_row = $row;
                        $candidate_row = \array_merge($left_row, $raw);

                        if (!$filter
                            || ExpressionEvaluator::evaluate($conn, $scope, $filter, $candidate_row, $joined_columns)
                        ) {
                            $rows[] = $candidate_row;
                            $any_match = true;
                        }
                    }

                    if (!$any_match) {
                        $rows[] = $raw;
                    }
                }
                break;

            case JoinType::CROSS:
                $joined_columns = array_merge($left_dataset[1], $right_dataset[1]);

                foreach ($left_dataset[0] as $row) {
                    foreach ($right_dataset[0] as $r) {
                        $left_row = $row;
                        $rows[] = \array_merge($left_row, $r);
                    }
                }

                break;

            case JoinType::NATURAL:
                $joined_columns = array_merge($left_dataset[1], $right_dataset[1]);

                $filter = self::buildNaturalJoinFilter($left_dataset[0], $right_dataset[0]);

                foreach ($left_dataset[0] as $row) {
                    foreach ($right_dataset[0] as $r) {
                        $left_row = $row;
                        $candidate_row = \array_merge($left_row, $r);
                        if (ExpressionEvaluator::evaluate($conn, $scope, $filter, $candidate_row, $joined_columns)) {
                            $rows[] = $candidate_row;
                        }
                    }
                }
                break;
        }

        return [$rows, $joined_columns];
    }

    /**
     * @param array<int, array<string, mixed>> $left_dataset
     * @param array<int, array<string, mixed>> $right_dataset
     *
     * @return Expression
     */
    protected static function buildNaturalJoinFilter(array $left_dataset, array $right_dataset) : Expression
    {
        $filter = null;
        $left = reset($left_dataset);
        $right = reset($right_dataset);

        if ($left === null || $right === null) {
            throw new SQLFakeParseException("Attempted NATURAL join with no data present");
        }

        foreach ($left as $column => $val) {
            $name_parts = \explode('.%.', $column);
            $name = end($name_parts);
            foreach ($right as $col => $v) {
                $col_parts = \explode('.%.', $col);
                $colname = end($col_parts);
                if ($colname === $name) {
                    $filter = self::addJoinFilterExpression($filter, $column, $col);
                }
            }
        }

        if ($filter === null) {
            throw new SQLFakeParseException(
                "NATURAL join keyword was used with tables that do not share any column names"
            );
        }

        return $filter;
    }

    /**
     * @return BinaryOperatorExpression
     */
    protected static function addJoinFilterExpression(
        ?Expression $filter,
        string $left_column,
        string $right_column
    ) {
        $left = new ColumnExpression(
            new Token(TokenType::IDENTIFIER, $left_column, $left_column)
        );
        $right = new ColumnExpression(
            new Token(TokenType::IDENTIFIER, $right_column, $right_column)
        );
        $expr = new BinaryOperatorExpression($left, false, '=', $right);

        if ($filter !== null) {
            $filter = new BinaryOperatorExpression($filter, false, 'AND', $expr);
        } else {
            $filter = $expr;
        }

        return $filter;
    }
}

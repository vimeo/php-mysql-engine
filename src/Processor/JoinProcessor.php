<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\Expression\Evaluator as ExpressionEvaluator;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;

final class JoinProcessor
{
    /**
     * @param array<int, array<string, mixed>> $left_dataset
     * @param array<int, array<string, mixed>> $right_dataset
     * @param JoinType::*                      $join_type
     * @param 'USING'|'OM'|null                $_ref_type
     *
     * @return array<int, array<string, mixed>>
     */
    public static function process(
        \Vimeo\MysqlEngine\FakePdo $conn,
        array $left_dataset,
        array $right_dataset,
        string $right_table_name,
        $join_type,
        $_ref_type,
        ?Expression $filter,
        \Vimeo\MysqlEngine\Schema\TableDefinition $right_table_definition
    ) {
        $out = [];

        switch ($join_type) {
            case JoinType::JOIN:
            case JoinType::STRAIGHT:
                foreach ($left_dataset as $row) {
                    foreach ($right_dataset as $r) {
                        $left_row = $row;
                        $candidate_row = \array_merge($row, $r);
                        if (!$filter || ExpressionEvaluator::evaluate($filter, $candidate_row, $conn)) {
                            $out[] = $candidate_row;
                        }
                    }
                }
                break;

            case JoinType::LEFT:
                $null_placeholder = [];
                foreach ($right_table_definition->columns as $name => $_) {
                    $null_placeholder["{$right_table_name}.{$name}"] = null;
                }

                foreach ($left_dataset as $row) {
                    $any_match = false;
                    foreach ($right_dataset as $r) {
                        $left_row = $row;
                        $candidate_row = \array_merge($left_row, $r);
                        if (!$filter || ExpressionEvaluator::evaluate($filter, $candidate_row, $conn)) {
                            $out[] = $candidate_row;
                            $any_match = true;
                        }
                    }
                    if (!$any_match) {
                        $out[] = \array_merge($row, $null_placeholder);
                    }
                }

                break;

            case JoinType::RIGHT:
                $null_placeholder = [];
                
                foreach ($right_table_definition->columns as $name => $_) {
                    $null_placeholder["{$right_table_name}.{$name}"] = null;
                }

                foreach ($right_dataset as $raw) {
                    $any_match = false;

                    foreach ($left_dataset as $row) {
                        $left_row = $row;
                        $candidate_row = \array_merge($left_row, $raw);
                        
                        if (!$filter || ExpressionEvaluator::evaluate($filter, $candidate_row, $conn)) {
                            $out[] = $candidate_row;
                            $any_match = true;
                        }
                    }

                    if (!$any_match) {
                        $out[] = $raw;
                    }
                }
                break;

            case JoinType::CROSS:
                foreach ($left_dataset as $row) {
                    foreach ($right_dataset as $r) {
                        $left_row = $row;
                        $out[] = \array_merge($left_row, $r);
                    }
                }
                break;

            case JoinType::NATURAL:
                $filter = self::buildNaturalJoinFilter($left_dataset, $right_dataset);
                foreach ($left_dataset as $row) {
                    foreach ($right_dataset as $r) {
                        $left_row = $row;
                        $candidate_row = \array_merge($left_row, $r);
                        if (ExpressionEvaluator::evaluate($filter, $candidate_row, $conn)) {
                            $out[] = $candidate_row;
                        }
                    }
                }
                break;
        }

        return $out;
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
            $name_parts = \explode('.', $column);
            $name = end($name_parts);
            foreach ($right as $col => $v) {
                $col_parts = \explode('.', $col);
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

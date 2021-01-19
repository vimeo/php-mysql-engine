<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\FakePdo;
use Vimeo\MysqlEngine\MultiOperand;
use Vimeo\MysqlEngine\Schema\Column;

final class SelectProcessor extends Processor
{
    /**
     * @param array<string, mixed>|null $row
     * @param array<string, Column>|null $columns
     *
     * @return QueryResult
     */
    public static function process(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        ?array $row = null,
        ?array $columns = null
    ) : QueryResult {
        $from = self::applyFrom(
            $conn,
            $scope,
            $stmt,
            $row,
            $columns
        );

        $where = self::applyWhere(
            $conn,
            $scope,
            $stmt->whereClause,
            $from
        );

        // apply preliminary ordering to match MySQL execcution order
        if ($scope->variables) {
            $where = self::applyOrderBy(
                $conn,
                $scope,
                $stmt->orderBy,
                $where
            );
        }

        $group_by = self::applyGroupBy(
            $conn,
            $scope,
            $stmt,
            $where
        );

        $having = self::applyHaving(
            $conn,
            $scope,
            $stmt,
            $group_by
        );

        $select = self::applySelect(
            $conn,
            $scope,
            $stmt,
            $having
        );

        $order_by = self::applyOrderBy(
            $conn,
            $scope,
            $stmt->orderBy,
            $select
        );

        return self::processMultiQuery(
            $conn,
            $scope,
            $stmt,
            self::removeOrderByExtras(
                $conn,
                $stmt,
                self::applyLimit(
                    $stmt->limitClause,
                    $order_by
                )
            )
        );
    }

    protected static function applyFrom(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        ?array $row,
        ?array $columns
    ) : QueryResult {
        $from = $stmt->fromClause;

        if (!$from) {
            return new QueryResult([], []);
        }

        $from_result = FromProcessor::process($conn, $scope, $stmt->fromClause);

        if ($row) {
            $from_result->rows = \array_map(
                function ($from_row) use ($row) {
                    return \array_merge($from_row, array_diff_key($row, $from_row));
                },
                $from_result->rows
            );
        }

        if ($columns) {
            $from_result->columns = array_merge($columns, $from_result->columns);
        }

        return $from_result;
    }

    protected static function applyGroupBy(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        QueryResult $result
    ) : QueryResult {
        $group_by = $stmt->groupBy;

        $select_expressions = $stmt->selectExpressions;

        if ($group_by !== null) {
            $rows = $result->rows;

            $grouped_rows = [];

            foreach ($rows as $row) {
                $hashes = '';

                foreach ($group_by as $expr) {
                    $hashes .= \sha1((string) Expression\Evaluator::evaluate($conn, $scope, $expr, $row, $result));
                }

                $hash = \sha1($hashes);

                if (!\array_key_exists($hash, $grouped_rows)) {
                    $grouped_rows[$hash] = [];
                }

                $count = \count($grouped_rows[$hash]);
                $grouped_rows[$hash][(string) $count] = $row;
            }

            return new QueryResult($rows, $result->columns, \array_values($grouped_rows));
        }

        foreach ($select_expressions as $expr) {
            if ($expr->hasAggregate()) {
                return new QueryResult($result->rows, $result->columns, [$result->rows]);
            }
        }

        return $result;
    }

    protected static function applyHaving(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        QueryResult $result
    ) : QueryResult {
        $havingClause = $stmt->havingClause;

        if ($havingClause !== null) {
            return new QueryResult(
                \array_filter(
                    $result->rows,
                    function ($row) use ($conn, $scope, $havingClause, $result) {
                        return (bool) Expression\Evaluator::evaluate($conn, $scope, $havingClause, $row, $result);
                    }
                ),
                $result->columns
            );
        }

        return $result;
    }

    protected static function applySelect(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        QueryResult $result
    ) : QueryResult {
        $columns = self::getSelectSchema($scope, $stmt, $result->columns, []);

        $order_by_expressions = $stmt->orderBy ?? [];

        foreach ($order_by_expressions as $order_by) {
            $columns[$order_by['expression']->name] = Expression\Evaluator::getColumnSchema(
                $order_by['expression'],
                $scope,
                $result->columns
            );
        }

        if (!$result->rows) {
            if ($stmt->fromClause
                && \array_filter(
                    $stmt->selectExpressions,
                    function ($expr) {
                        return !$expr->hasAggregate();
                    }
                )
            ) {
                return new QueryResult([], $columns);
            }

            $formatted_row = [];

            foreach ($stmt->selectExpressions as $expr) {
                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, [], $result);
                $name = $expr->name;

                $formatted_row[$name] = $val;
            }

            return new QueryResult([$formatted_row], $columns);
        }

        $out = [];

        $i = 0;

        $grouped_rows = $result->grouped_rows !== null ? $result->grouped_rows : [$result->rows];

        foreach ($grouped_rows as $group_id => $rows) {
            $group_result = $result->grouped_rows !== null
                ? new QueryResult($rows, $result->columns)
                : $result;

            foreach ($rows as $row) {
                $found_aggregate = false;

                foreach ($stmt->selectExpressions as $expr) {
                    if ($expr instanceof ColumnExpression && $expr->name === '*') {
                        $formatted_row = [];

                        $first_value = \reset($row);

                        if (\is_array($first_value)) {
                            $row = $first_value;
                        }

                        foreach ($row as $col => $val) {
                            $parts = \explode(".%.", (string) $col);

                            if ($expr->tableName() !== null) {
                                list($col_table_name, $col_name) = $parts;
                                if ($col_table_name == $expr->tableName()) {
                                    if (!\array_key_exists($col, $formatted_row)) {
                                        $formatted_row[$col_name] = $val;
                                    }
                                }
                            } else {
                                $col_name = \end($parts);
                                $val = $formatted_row[$col_name] ?? $val;

                                $formatted_row[$col_name] = $val;
                            }
                        }

                        $out[$i] = $formatted_row;

                        continue;
                    }

                    $val = Expression\Evaluator::evaluate($conn, $scope, $expr, $row, $group_result);
                    $name = $expr->name;

                    if ($expr instanceof SubqueryExpression) {
                        assert(\is_array($val), 'subquery results must be KeyedContainer');
                        if (\count($val) > 1) {
                            throw new SQLFakeRuntimeException("Subquery returned more than one row");
                        }
                        if (\count($val) === 0) {
                            $val = null;
                        } else {
                            foreach ($val as $r) {
                                if (\count($r) !== 1) {
                                    throw new SQLFakeRuntimeException("Subquery result should contain 1 column");
                                }
                                $val = \reset($r);
                            }
                        }
                    }

                    $out[$i][$name] = $val;

                    if ($expr->hasAggregate()) {
                        $found_aggregate = true;
                    }
                }

                if ($scope->variables) {
                    // fetch columns again if we have temp variables
                    $columns = self::getSelectSchema($scope, $stmt, $result->columns, $columns);
                }

                $i++;

                if ($found_aggregate || $result->grouped_rows !== null) {
                    break;
                }
            }
        }

        if ($i === 0 && $result->grouped_rows !== null) {
            foreach ($stmt->selectExpressions as $expr) {
                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, [], $result);
                $name = $expr->name;

                if ($expr instanceof SubqueryExpression) {
                    assert(\is_array($val), 'subquery results must be KeyedContainer');
                    if (\count($val) > 1) {
                        throw new SQLFakeRuntimeException("Subquery returned more than one row");
                    }
                    if (\count($val) === 0) {
                        $val = null;
                    } else {
                        foreach ($val as $r) {
                            if (\count($r) !== 1) {
                                throw new SQLFakeRuntimeException("Subquery result should contain 1 column");
                            }
                            $val = \reset($r);
                        }
                    }
                }

                $out[$i][$name] = $val;

                if ($expr->hasAggregate()) {
                    $found_aggregate = true;
                }
            }
        }

        $i = 0;

        foreach ($grouped_rows as $group_id => $rows) {
            $group_result = $result->grouped_rows !== null
                ? new QueryResult($rows, $result->columns)
                : $result;

            foreach ($rows as $row) {
                $found_aggregate = false;

                foreach ($order_by_expressions as $order_by) {
                    $val = Expression\Evaluator::evaluate($conn, $scope, $order_by['expression'], $row, $result);
                    $name = $order_by['expression']->name;
                    $out[$i][$name] = $out[$i][$name] ?? $val;

                    if ($order_by['expression']->hasAggregate()) {
                        $found_aggregate = true;
                    }
                }

                $i++;

                if ($found_aggregate || $result->grouped_rows !== null) {
                    break;
                }
            }
        }

        $out = array_values($out);

        if (\array_key_exists('DISTINCT', $stmt->options)) {
            $new_out = [];

            foreach ($out as $row) {
                $key = \implode(
                    '-',
                    \array_map(
                        function ($col) {
                            return (string) $col;
                        },
                        $row
                    )
                );

                if (!array_key_exists($key, $new_out)) {
                    $new_out[$key] = $row;
                }
            }

            return new QueryResult(array_values($new_out), $columns);
        }

        return new QueryResult($out, $columns);
    }

    private static function getSelectSchema(
        Scope $scope,
        SelectQuery $stmt,
        array $from_columns,
        array $existing_columns
    ) : array {
        $columns = [];

        foreach ($stmt->selectExpressions as $expr) {
            if ($expr instanceof ColumnExpression && $expr->name === '*') {
                foreach ($from_columns as $column_id => $from_column) {
                    $parts = \explode(".", $column_id);

                    if ($expr_table_name = $expr->tableName()) {
                        list($column_table_name, $column_name) = $parts;

                        if ($column_table_name === $expr_table_name) {
                            $columns[$column_id] = $from_column;
                        }
                    } else {
                        $col_name = \end($parts);

                        $columns[$col_name] = $from_column;
                    }
                }
            } else {
                if (!isset($existing_columns[$expr->name])) {
                    $columns[$expr->name] = Expression\Evaluator::getColumnSchema($expr, $scope, $from_columns);
                } elseif ($existing_columns[$expr->name] instanceof Column\NullColumn) {
                    $columns[$expr->name] = clone Expression\Evaluator::getColumnSchema($expr, $scope, $from_columns);
                    $columns[$expr->name]->isNullable = true;
                }
            }
        }

        return \array_merge($existing_columns, $columns);
    }

    protected static function removeOrderByExtras(
        FakePdo $_conn,
        SelectQuery $stmt,
        QueryResult $result
    ) : QueryResult {
        $order_by = $stmt->orderBy;

        if ($order_by === null || \count($result->rows) === 0) {
            return $result;
        }

        $order_by_names = [];
        $select_field_names = [];

        foreach ($stmt->selectExpressions as $expr) {
            $name = $expr->name;

            if ($name == "*") {
                return $result;
            }

            if ($name !== null) {
                $select_field_names[$name] = true;
            }
        }

        foreach ($order_by as $o) {
            $name = $o['expression']->name;

            if ($name !== null) {
                $order_by_names[$name] = true;
            }
        }

        $remove_fields = \array_diff_key($order_by_names, $select_field_names);

        if (0 === \count($remove_fields)) {
            return $result;
        }

        return new QueryResult(
            \array_map(
                function ($row) use ($remove_fields) {
                    return \array_filter(
                        $row,
                        function ($field) use ($remove_fields) {
                            return !\array_key_exists($field, $remove_fields);
                        },
                        \ARRAY_FILTER_USE_KEY
                    );
                },
                $result->rows
            ),
            array_diff_key($result->columns, $remove_fields)
        );
    }

    protected static function processMultiQuery(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        QueryResult $result
    ) {
        $row_encoder = function ($row) {
            return \implode(
                '-',
                \array_map(
                    function ($col) {
                        return (string) $col;
                    },
                    $row
                )
            );
        };

        $rows = $result->rows;

        foreach ($stmt->multiQueries as $sub) {
            $subquery_result = SelectProcessor::process($conn, $scope, $sub['query'], null);

            switch ($sub['type']) {
                case MultiOperand::UNION:
                    $deduped_rows = [];
                    foreach (\array_merge($subquery_result->rows, $rows) as $row) {
                        $deduped_rows[$row_encoder($row)] = $row;
                    }
                    $rows = array_values($deduped_rows);
                    break;

                case MultiOperand::UNION_ALL:
                    $rows = \array_merge($subquery_result->rows, $rows);
                    break;

                case MultiOperand::INTERSECT:
                    $encoded_rows = \array_map($row_encoder, $rows);
                    $rows = \array_filter(
                        $subquery_result->rows,
                        function ($row) use ($encoded_rows, $row_encoder) {
                            return \in_array($row_encoder($row), $encoded_rows);
                        }
                    );
                    break;

                case MultiOperand::EXCEPT:
                    $encoded_subquery = \array_map($row_encoder, $subquery_result->rows);
                    $rows = \array_filter(
                        $rows,
                        function ($row) use ($encoded_subquery, $row_encoder) {
                            return !\in_array($row_encoder($row), $encoded_subquery);
                        }
                    );
                    break;
            }
        }

        return new QueryResult($rows, $result->columns);
    }
}

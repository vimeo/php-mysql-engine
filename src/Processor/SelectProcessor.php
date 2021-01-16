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
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    public static function process(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        ?array $row = null,
        ?array $columns = null
    ) : array {
        return self::processMultiQuery(
            $conn,
            $scope,
            $stmt,
            self::removeOrderByExtras(
                $conn,
                $stmt,
                self::applyLimit(
                    $stmt->limitClause,
                    self::applyOrderBy(
                        $conn,
                        $scope,
                        $stmt->orderBy,
                        self::applySelect(
                            $conn,
                            $scope,
                            $stmt,
                            self::applyHaving(
                                $conn,
                                $scope,
                                $stmt,
                                self::applyGroupBy(
                                    $conn,
                                    $scope,
                                    $stmt,
                                    self::applyWhere(
                                        $conn,
                                        $scope,
                                        $stmt->whereClause,
                                        self::applyFrom($conn, $scope, $stmt, $row, $columns)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );
    }

    /**
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyFrom(
        FakePdo $conn,
        Scope $scope,
        SelectQuery $stmt,
        ?array $row,
        ?array $columns
    ) {
        $from = $stmt->fromClause;

        if (!$from) {
            return [[], []];
        }

        [$from_rows, $from_columns] = FromProcessor::process($conn, $scope, $stmt->fromClause);

        if ($row) {
            $from_rows = \array_map(
                function ($from_row) use ($row) {
                    return \array_merge($from_row, array_diff_key($row, $from_row));
                },
                $from_rows
            );
        }

        if ($columns) {
            $from_columns = array_merge($columns, $from_columns);
        }

        return [$from_rows, $from_columns];
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyGroupBy(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data)
    {
        $group_by = $stmt->groupBy;

        $select_expressions = $stmt->selectExpressions;

        if ($group_by !== null) {
            $rows = $data[0];

            $grouped_rows = [];

            foreach ($rows as $row) {
                $hashes = '';

                foreach ($group_by as $expr) {
                    $hashes .= \sha1((string) Expression\Evaluator::evaluate($conn, $scope, $expr, $row, $data[1]));
                }

                $hash = \sha1($hashes);

                if (!\array_key_exists($hash, $grouped_rows)) {
                    $grouped_rows[$hash] = [];
                }

                $count = \count($grouped_rows[$hash]);
                $grouped_rows[$hash][(string) $count] = $row;
            }

            return [\array_values($grouped_rows), $data[1]];
        }

        $found_aggregate = false;

        foreach ($select_expressions as $expr) {
            if ($expr->hasAggregate()) {
                $found_aggregate = true;
                break;
            }
        }

        if ($found_aggregate) {
            return [[$data[0]], $data[1]];
        }

        return $data;
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyHaving(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data)
    {
        $havingClause = $stmt->havingClause;

        if ($havingClause !== null) {
            return [
                \array_filter(
                    $data[0],
                    function ($row) use ($conn, $scope, $havingClause, $data) {
                        return (bool) Expression\Evaluator::evaluate($conn, $scope, $havingClause, $row, $data[1]);
                    }
                ),
                $data[1]
            ];
        }

        return $data;
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applySelect(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data) : array
    {
        $columns = [];

        foreach ($stmt->selectExpressions as $expr) {
            if ($expr instanceof ColumnExpression && $expr->name === '*') {
                foreach ($data[1] as $column_id => $existing_column) {
                    $parts = \explode(".", $column_id);

                    if ($expr_table_name = $expr->tableName()) {
                        list($column_table_name, $column_name) = $parts;

                        if ($column_table_name === $expr_table_name) {
                            $columns[$column_id] = $existing_column;
                        }
                    } else {
                        $col_name = \end($parts);

                        $columns[$col_name] = $existing_column;
                    }
                }
            } else {
                $columns[$expr->name] = Expression\Evaluator::getColumnSchema($expr, $scope, $data[1]);
            }
        }

        $order_by_expressions = $stmt->orderBy ?? [];

        foreach ($order_by_expressions as $order_by) {
            $columns[$order_by['expression']->name] = Expression\Evaluator::getColumnSchema(
                $order_by['expression'],
                $scope,
                $data[1]
            );
        }

        if (!$data[0]) {
            if ($stmt->fromClause) {
                return [[], $columns];
            }

            $formatted_row = [];

            foreach ($stmt->selectExpressions as $expr) {
                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, [], $data[1]);
                $name = $expr->name;

                $formatted_row[$name] = $val;
            }

            return [[$formatted_row], $columns];
        }

        $out = [];

        foreach ($data[0] as $i => $row) {
            foreach ($stmt->selectExpressions as $expr) {
                if ($expr instanceof ColumnExpression && $expr->name === '*') {
                    $formatted_row = [];

                    $first_value = \reset($row);

                    if (\is_array($first_value)) {
                        $row = $first_value;
                    }

                    foreach ($row as $col => $val) {
                        $parts = \explode(".", (string) $col);

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

                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, $row, $data[1]);
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
            }
        }

        foreach ($order_by_expressions as $order_by) {
            foreach ($data[0] as $i => $row) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();
                $val = Expression\Evaluator::evaluate($conn, $scope, $order_by['expression'], $row, $data[1]);
                $name = $order_by['expression']->name;
                $out[$i][$name] = $out[$i][$name] ?? $val;
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

            return [array_values($new_out), $columns];
        }

        return [$out, $columns];
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function removeOrderByExtras(FakePdo $_conn, SelectQuery $stmt, array $data)
    {
        $order_by = $stmt->orderBy;

        if ($order_by === null || \count($data[0]) === 0) {
            return $data;
        }

        $order_by_names = [];
        $select_field_names = [];

        foreach ($stmt->selectExpressions as $expr) {
            $name = $expr->name;

            if ($name == "*") {
                return $data;
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
            return $data;
        }

        return [
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
                $data[0]
            ),
            array_diff_key($data[1], $remove_fields)
        ];
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function processMultiQuery(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data)
    {
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

        $rows = $data[0];

        foreach ($stmt->multiQueries as $sub) {
            [$subquery_results, $subquery_columns] = SelectProcessor::process($conn, $scope, $sub['query'], null);

            switch ($sub['type']) {
                case MultiOperand::UNION:
                    $deduped_rows = [];
                    foreach (\array_merge($subquery_results, $rows) as $row) {
                        $deduped_rows[$row_encoder($row)] = $row;
                    }
                    $rows = array_values($deduped_rows);
                    break;

                case MultiOperand::UNION_ALL:
                    $rows = \array_merge($subquery_results, $rows);
                    break;

                case MultiOperand::INTERSECT:
                    $encoded_rows = \array_map($row_encoder, $rows);
                    $rows = \array_filter(
                        $subquery_results,
                        function ($row) use ($encoded_rows, $row_encoder) {
                            return \in_array($row_encoder($row), $encoded_rows);
                        }
                    );
                    break;

                case MultiOperand::EXCEPT:
                    $encoded_subquery = \array_map($row_encoder, $subquery_results);
                    $rows = \array_filter(
                        $rows,
                        function ($row) use ($encoded_subquery, $row_encoder) {
                            return !\in_array($row_encoder($row), $encoded_subquery);
                        }
                    );
                    break;
            }
        }

        return [$rows, $data[1]];
    }
}

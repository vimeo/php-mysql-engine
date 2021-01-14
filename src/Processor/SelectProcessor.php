<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\FakePdo;
use Vimeo\MysqlEngine\MultiOperand;

final class SelectProcessor extends Processor
{
    /**
     * @param array<string, mixed>|null $row
     *
     * @return array<int, array<string, mixed>>
     */
    public static function process(FakePdo $conn, Scope $scope, SelectQuery $stmt, ?array $row) : array
    {
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
                                        self::applyFrom($conn, $scope, $stmt, $row)
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
     * @return array<int, array<string, mixed>>
     */
    protected static function applyFrom(FakePdo $conn, Scope $scope, SelectQuery $stmt, ?array $row)
    {
        $from = $stmt->fromClause;

        if (!$from) {
            return [];
        }

        $from_rows = FromProcessor::process($conn, $scope, $stmt->fromClause);

        if ($row) {
            $from_rows = \array_map(
                function ($from_row) use ($row) {
                    return \array_merge($from_row, array_diff_key($row, $from_row));
                },
                $from_rows
            );
        }

        return $from_rows;
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyGroupBy(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data)
    {
        $group_by = $stmt->groupBy;

        $select_expressions = $stmt->selectExpressions;

        if ($group_by !== null) {
            $grouped_data = [];

            foreach ($data as $row) {
                $hashes = '';

                foreach ($group_by as $expr) {
                    $hashes .= \sha1((string) Expression\Evaluator::evaluate($conn, $scope, $expr, $row));
                }

                $hash = \sha1($hashes);

                if (!\array_key_exists($hash, $grouped_data)) {
                    $grouped_data[$hash] = [];
                }

                $count = \count($grouped_data[$hash]);
                $grouped_data[$hash][(string) $count] = $row;
            }

            return \array_values($grouped_data);
        }

        $found_aggregate = false;

        foreach ($select_expressions as $expr) {
            if ($expr->hasAggregate()) {
                $found_aggregate = true;
                break;
            }
        }

        if ($found_aggregate) {
            return [$data];
        }

        return $data;
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyHaving(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data)
    {
        $havingClause = $stmt->havingClause;

        if ($havingClause !== null) {
            return \array_filter(
                $data,
                function ($row) use ($conn, $scope, $havingClause) {
                    return (bool) Expression\Evaluator::evaluate($conn, $scope, $havingClause, $row);
                }
            );
        }

        return $data;
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applySelect(FakePdo $conn, Scope $scope, SelectQuery $stmt, array $data) : array
    {
        $out = [];

        if (!$data) {
            if ($stmt->fromClause) {
                return [];
            }

            $formatted_row = [];

            foreach ($stmt->selectExpressions as $expr) {
                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, []);
                $name = $expr->name;

                $formatted_row[\substr($name, 0, 255)] = $val;
            }

            return [$formatted_row];
        }

        $order_by_expressions = $stmt->orderBy ?? [];

        foreach ($data as $row) {
            $formatted_row = [];

            foreach ($stmt->selectExpressions as $expr) {
                if ($expr instanceof ColumnExpression && $expr->name === '*') {
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

                            $formatted_row[\substr($col_name, 0, 255)] = $val;
                        }
                    }

                    continue;
                }

                $val = Expression\Evaluator::evaluate($conn, $scope, $expr, $row);
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

                $formatted_row[\substr($name, 0, 255)] = $val;
            }

            foreach ($order_by_expressions as $order_by) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();
                $val = Expression\Evaluator::evaluate($conn, $scope, $order_by['expression'], $row);
                $name = $order_by['expression']->name;
                $formatted_row[\substr($name, 0, 255)] = $formatted_row[$name] ?? $val;
            }

            $out[] = $formatted_row;
        }

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
                    $new_out[\substr($key, 0, 255)] = $row;
                }
            }

            return array_values($new_out);
        }

        return $out;
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function removeOrderByExtras(FakePdo $_conn, SelectQuery $stmt, array $data)
    {
        $order_by = $stmt->orderBy;

        if ($order_by === null || \count($data) === 0) {
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

        return \array_map(
            function ($row) use ($remove_fields) {
                return \array_filter(
                    $row,
                    function ($field) use ($remove_fields) {
                        return !\array_key_exists($field, $remove_fields);
                    },
                    \ARRAY_FILTER_USE_KEY
                );
            },
            $data
        );
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
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

        foreach ($stmt->multiQueries as $sub) {
            $subquery_results = SelectProcessor::process($conn, $scope, $sub['query'], null);

            switch ($sub['type']) {
                case MultiOperand::UNION:
                    $deduped_data = [];
                    foreach (\array_merge($subquery_results, $data) as $row) {
                        $deduped_data[$row_encoder($row)] = $row;
                    }
                    $data = array_values($deduped_data);
                    break;

                case MultiOperand::UNION_ALL:
                    $data = \array_merge($subquery_results, $data);
                    break;

                case MultiOperand::INTERSECT:
                    $encoded_data = \array_map($row_encoder, $data);
                    $data = \array_filter(
                        $subquery_results,
                        function ($row) use ($encoded_data, $row_encoder) {
                            return \in_array($row_encoder($row), $encoded_data);
                        }
                    );
                    break;

                case MultiOperand::EXCEPT:
                    $encoded_subquery = \array_map($row_encoder, $subquery_results);
                    $data = \array_filter(
                        $data,
                        function ($row) use ($encoded_subquery, $row_encoder) {
                            return !\in_array($row_encoder($row), $encoded_subquery);
                        }
                    );
                    break;
            }
        }

        return $data;
    }
}

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
     * @param array<string, mixed>|null $_1
     *
     * @return array<int, array<string, mixed>>
     */
    public static function process(FakePdo $conn, SelectQuery $stmt, ?array $_1 = null) : array
    {
        return self::processMultiQuery(
            $conn,
            $stmt,
            self::removeOrderByExtras(
                $conn,
                $stmt,
                self::applyLimit(
                    $stmt->limitClause,
                    self::applyOrderBy(
                        $conn,
                        $stmt->orderBy,
                        self::applySelect(
                            $conn,
                            $stmt,
                            self::applyHaving(
                                $conn,
                                $stmt,
                                self::applyGroupBy(
                                    $conn,
                                    $stmt,
                                    self::applyWhere(
                                        $conn,
                                        $stmt->whereClause,
                                        self::applyFrom($conn, $stmt)
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
    protected static function applyFrom(FakePdo $conn, SelectQuery $stmt)
    {
        $from = $stmt->fromClause;

        if (!$from) {
            return [];
        }

        return FromProcessor::process($conn, $stmt->fromClause);
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyGroupBy(FakePdo $conn, SelectQuery $stmt, array $data)
    {
        $group_by = $stmt->groupBy;

        $select_expressions = $stmt->selectExpressions;

        if ($group_by !== null) {
            $grouped_data = [];

            foreach ($data as $row) {
                $hashes = '';

                foreach ($group_by as $expr) {
                    $hashes .= \sha1((string) Expression\Evaluator::evaluate($expr, $row, $conn));
                }

                $hash = \sha1($hashes);

                if (!\array_key_exists($hash, $grouped_data)) {
                    $grouped_data[$hash] = [];
                }

                $count = \count($grouped_data[$hash]);
                $grouped_data[$hash][(string) $count] = $row;
            }

            $data = (array) $grouped_data;
        } else {
            $found_aggregate = false;

            foreach ($select_expressions as $expr) {
                if ($expr instanceof FunctionExpression && $expr->isAggregate()) {
                    $found_aggregate = true;
                    break;
                }
            }

            if ($found_aggregate) {
                return [$data];
            }
        }
        return $data;
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyHaving(FakePdo $conn, SelectQuery $stmt, array $data)
    {
        $havingClause = $stmt->havingClause;

        if ($havingClause !== null) {
            return \array_filter(
                $data, function ($row) use ($conn, $havingClause) {
                    return (bool) Expression\Evaluator::evaluate($havingClause, $row, $conn);
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
    protected static function applySelect(FakePdo $conn, SelectQuery $stmt, array $data) : array
    {
        $order_by_expressions = $stmt->orderBy ?? [];
        $out = [];

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
                            $col = \end($parts);
                            if ($col !== null) {
                                $val = $formatted_row[$col] ?? $val;

                                $formatted_row[$col] = $val;
                            }
                        }
                    }

                    continue;
                }

                $val = Expression\Evaluator::evaluate($expr, $row, $conn);
                $name = $expr->name;

                if ($expr instanceof SubqueryExpression) {
                    assert(\is_iterable($val), 'subquery results must be KeyedContainer');
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
                            $val = C\onlyx($r);
                        }
                    }
                }

                $formatted_row[$name] = $val;
            }

            foreach ($order_by_expressions as $order_by) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();
                $val = Expression\Evaluator::evaluate($order_by['expression'], $row, $conn);
                $name = $order_by['expression']->name;
                $formatted_row[$name] = $formatted_row[$name] ?? $val;
            }

            $out[] = $formatted_row;
        }

        if (\array_key_exists('DISTINCT', $stmt->options)) {
            $new_out = [];

            foreach ($out as $row) {
                $key = \implode('-', \array_map(fn($col) => (string) $col, $row));

                if (!array_key_exists($key, $new_out)) {
                    $new_out[$key] = $row;
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
            function ($row) {
                return \array_filter(
                    $row,
                    fn ($field) => !\array_key_exists($field, $remove_fields),
                    \ARRAY_FILTER_USE_KEY
                );
            }, $data
        );
    }

    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function processMultiQuery(FakePdo $conn, SelectQuery $stmt, array $data)
    {
        $row_encoder = fn($row) => \implode('-', \array_map(fn($col) => (string) $col, $row));

        foreach ($stmt->union as $sub) {
            $subquery_results = $sub['query']->execute($conn);

            switch ($sub['type']) {
                case MultiOperand::UNION:
                    $data = Vec\unique_by(\array_merge($subquery_results, $data), $row_encoder);
                    break;

                case MultiOperand::UNION_ALL:
                    $data = \array_merge($subquery_results, $data);
                    break;

                case MultiOperand::INTERSECT:
                    $encoded_data = Keyset\map($data, $row_encoder);
                    $data = \array_filter(
                        $subquery_results, function ($row) use ($encoded_data, $row_encoder) {
                            return \in_array($row_encoder($row), $encoded_data);
                        }
                    );
                    break;

                case MultiOperand::EXCEPT:
                    $data = Vec\diff_by($data, $subquery_results, $row_encoder);
                    break;
            }
        }

        return $data;
    }
}

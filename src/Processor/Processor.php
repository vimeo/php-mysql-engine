<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataIntegrity;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Schema\Column\IntegerColumn;
use Vimeo\MysqlEngine\Schema\TableDefinition;

abstract class Processor
{
    /**
     * @param array<int, array<string, mixed>> $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyWhere(
        \Vimeo\MysqlEngine\FakePdo $conn,
        ?\Vimeo\MysqlEngine\Query\Expression\Expression $where,
        array $data
    ) {
        if (!$where) {
            return $data;
        }

        return \array_filter(
            $data,
            fn($row) => Expression\Evaluator::evaluate($where, $row, $conn)
        );
    }

    /**
     * @param array<int, array<string, mixed>>                                                                  $data
     * @param ?array<int, array{expression: \Vimeo\MysqlEngine\Query\Expression\Expression, direction: string}> $orders
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyOrderBy(
        \Vimeo\MysqlEngine\FakePdo $conn,
        ?array $orders,
        array $data
    ) {
        if (!$orders) {
            return $data;
        }

        foreach ($orders as $rule) {
            $expr = $rule['expression'];
            if ($expr instanceof \PhpMyAdmin\SqlParser\Components\Reference) {
                $expr->allowFallthrough();
            }
        }
        $sort_fun = function (array $a, array $b) use ($orders, $conn) {
            foreach ($orders as $rule) {
                $value_a = Expression\Evaluator::evaluate($rule['expression'], $a, $conn);
                $value_b = Expression\Evaluator::evaluate($rule['expression'], $b, $conn);

                if ($value_a != $value_b) {
                    if ((\is_int($value_a) || \is_float($value_a)) && (\is_int($value_b) || \is_float($value_b))) {
                        return ((double) $value_a < (double) $value_b ? 1 : 0)
                            ^ ($rule['direction'] === 'DESC' ? 1 : 0) ? -1 : 1;
                    } else {
                        return ((string) $value_a < (string) $value_b ? 1 : 0)
                            ^ ($rule['direction'] === 'DESC' ? 1 : 0) ? -1 : 1;
                    }
                }
            }

            return 0;
        };

        $data_temp = [];
        foreach ($data as $i => $item) {
            $data_temp[$i] = [$i, $item];
        }

        \usort(
            $data_temp,
            function ($a, $b) use ($sort_fun) {
                $result = $sort_fun($a[1], $b[1]);
                return $result === 0 ? $b[0] - $a[0] : $result;
            }
        );

        $data = [];
        foreach ($data_temp as $index => $item) {
            $data[$item[0]] = $item[1];
        }
        return array_values($data);
    }

    /**
     * @param array{rowcount:int, offset:int}|null $limit
     * @param array<int, array<string, mixed>>     $data
     *
     * @return array<int, array<string, mixed>>
     */
    protected static function applyLimit(?array $limit, array $data)
    {
        if ($limit === null) {
            return $data;
        }

        return \array_slice($data, $limit['offset'], $limit['rowcount'], true);
    }

    /**
     * @return array{0:string, 1:string}
     */
    public static function parseTableName(\Vimeo\MysqlEngine\FakePdo $conn, string $table)
    {
        if (\strpos($table, '.')) {
            $parts = \explode('.', $table);
            if (\count($parts) !== 2) {
                throw new SQLFakeRuntimeException("Table name {$table} has too many parts");
            }
            list($database, $table_name) = $parts;
            return [$database, $table_name];
        } else {
            $database = $conn->databaseName;
            return [$database, $table];
        }
    }

    /**
     * @param array<int, array<string, mixed>>                                   $filtered_rows
     * @param array<int, array<string, mixed>>                                   $original_table
     * @param list<\Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression> $set_clause
     * @param array<string, mixed>|null                                          $values
     *
     * @return array{0:int, 1:array<int, array<string, mixed>>}
     */
    protected static function applySet(
        \Vimeo\MysqlEngine\FakePdo $conn,
        string $database,
        string $table_name,
        ?array $filtered_rows,
        array $original_table,
        array $set_clause,
        TableDefinition $table_definition,
        ?array $values = null
    ) {
        $valid_fields = $table_definition->columns ?? null;

        $set_clauses = [];

        foreach ($set_clause as $expression) {
            $left = ($__tmp1__ = $expression->left) instanceof ColumnExpression ? $__tmp1__ : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $right = ($__tmp2__ = $expression->right) !== null ? $__tmp2__ : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $column = $left->name;

            if ($valid_fields !== null) {
                if (!isset($valid_fields[$column])) {
                    throw new SQLFakeRuntimeException("Invalid update column {$column}");
                }
            }

            $set_clauses[] = ['column' => $column, 'expression' => $right];
        }

        $update_count = 0;

        $last_insert_id = null;

        if ($filtered_rows !== null) {
            foreach ($filtered_rows as $row_id => $row) {
                $changes_found = false;
                $update_row = $row;

                if ($values !== null) {
                    foreach ($values as $col => $val) {
                        $update_row['sql_fake_values.' . $col] = $val;
                    }
                }

                foreach ($set_clauses as $clause) {
                    $existing_value = $row[$clause['column']] ?? null;
                    $new_value = Expression\Evaluator::evaluate($clause['expression'], $update_row, $conn);
                    if ($new_value !== $existing_value) {
                        $row[$clause['column']] = $new_value;
                        $changes_found = true;
                    }
                }

                if ($changes_found) {
                    $row = DataIntegrity::coerceToSchema($conn, $row, $table_definition);
                    $result = DataIntegrity::checkUniqueConstraints($original_table, $row, $table_definition, $row_id);

                    if ($result !== null) {
                        throw new SQLFakeUniqueKeyViolation($result[0]);
                    }

                    $original_table[$row_id] = $row;
                    $update_count++;
                }
            }
        } else {
            $changes_found = true;
            $row = [];

            foreach ($set_clauses as $clause) {
                $row[$clause['column']] = Expression\Evaluator::evaluate($clause['expression'], [], $conn);
            }

            foreach ($row as $column_name => $value) {
                $column = $table_definition->columns[$column_name];

                if ($column instanceof IntegerColumn && $column->isAutoIncrement()) {
                    $conn->getServer()->addAutoIncrementMinValue(
                        $database,
                        $table_name,
                        $column_name,
                        $value
                    );
                }
            }

            $row = DataIntegrity::coerceToSchema($conn, $row, $table_definition);

            if (\count($table_definition->primaryKeyColumns) === 1) {
                $last_insert_id = $row[$table_definition->primaryKeyColumns[0]];
            }

            $result = DataIntegrity::checkUniqueConstraints($original_table, $row, $table_definition, null);

            if ($result !== null) {
                throw new SQLFakeUniqueKeyViolation($result[0]);
            }

            $original_table[] = $row;
            $update_count++;
        }

        $conn->getServer()->saveTable($database, $table_name, $original_table);

        $conn->lastInsertId = $last_insert_id;

        return [$update_count, $original_table, ];
    }
}

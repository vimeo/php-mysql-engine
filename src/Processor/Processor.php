<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataIntegrity;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Schema\Column\IntegerColumn;
use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Schema\Column;

abstract class Processor
{
    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyWhere(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        ?\Vimeo\MysqlEngine\Query\Expression\Expression $where,
        array $data
    ) {
        if (!$where) {
            return $data;
        }

        return [
            \array_filter(
                $data[0],
                function ($row) use ($conn, $scope, $where, $data) {
                    return Expression\Evaluator::evaluate($conn, $scope, $where, $row, $data[1]);
                }
            ),
            $data[1]
        ];
    }

    /**
     * @param array{array<int, array<string, mixed>>, array<string, Column>} $data
     * @param ?array<int, array{expression: \Vimeo\MysqlEngine\Query\Expression\Expression, direction: string}> $orders
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyOrderBy(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        ?array $orders,
        array $data
    ) {
        if (!$orders) {
            return $data;
        }

        // allow all column expressions to fall through to the full row
        foreach ($orders as $rule) {
            $expr = $rule['expression'];

            if ($expr instanceof ColumnExpression) {
                $expr->allowFallthrough();
            }
        }

        $sort_fun = function (array $a, array $b) use ($conn, $scope, $orders, $data) {
            foreach ($orders as $rule) {
                $value_a = Expression\Evaluator::evaluate($conn, $scope, $rule['expression'], $a, $data[1]);
                $value_b = Expression\Evaluator::evaluate($conn, $scope, $rule['expression'], $b, $data[1]);

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

        $rows = $data[0];

        $rows_temp = [];
        foreach ($rows as $i => $item) {
            $rows_temp[$i] = [$i, $item];
        }

        \usort(
            $rows_temp,
            function ($a, $b) use ($sort_fun) {
                $result = $sort_fun($a[1], $b[1]);
                return $result === 0 ? $b[0] - $a[0] : $result;
            }
        );

        $rows = [];
        foreach ($rows_temp as $index => $item) {
            $rows[$item[0]] = $item[1];
        }

        return [array_values($rows), $data[1]];
    }

    /**
     * @param array{rowcount:int, offset:int}|null $limit
     * @param array{array<int, array<string, mixed>>, array<string, Column>}    $data
     *
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    protected static function applyLimit(?array $limit, array $data)
    {
        if ($limit === null) {
            return $data;
        }

        return [
            \array_slice($data[0], $limit['offset'], $limit['rowcount'], true),
            $data[1]
        ];
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
        Scope $scope,
        string $database,
        string $table_name,
        ?array $filtered_rows,
        array $original_table,
        array $set_clause,
        TableDefinition $table_definition,
        ?array $values = null
    ) {
        $valid_fields = $table_definition->columns;

        $set_clauses = [];

        foreach ($set_clause as $expression) {
            if (!$expression->left instanceof ColumnExpression || !$expression->right instanceof ColumnExpression) {
                throw new \TypeError('Failed assertion');
            }

            $column = $expression->left->columnName;

            if (!isset($valid_fields[$column])) {
                throw new SQLFakeRuntimeException("Invalid update column {$column}");
            }

            $set_clauses[] = ['column' => $column, 'expression' => $expression->right];
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
                    $new_value = Expression\Evaluator::evaluate(
                        $conn,
                        $scope,
                        $clause['expression'],
                        $update_row,
                        $table_definition->columns
                    );

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
                $row[$clause['column']] = Expression\Evaluator::evaluate(
                    $conn,
                    $scope,
                    $clause['expression'],
                    [],
                    $table_definition->columns
                );
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

        $conn->lastInsertId = (string) $last_insert_id;

        return [$update_count, $original_table, ];
    }
}

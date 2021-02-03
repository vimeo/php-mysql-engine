<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataIntegrity;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\NamedPlaceholderExpression;
use Vimeo\MysqlEngine\Query\LimitClause;
use Vimeo\MysqlEngine\Schema\Column\IntegerColumn;
use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Schema\Column;

abstract class Processor
{
    protected static function applyWhere(
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        ?\Vimeo\MysqlEngine\Query\Expression\Expression $where,
        QueryResult $result
    ) : QueryResult {
        if (!$where) {
            return $result;
        }

        $rows = [];

        foreach ($result->rows as $i => $row) {
            if (Expression\Evaluator::evaluate($conn, $scope, $where, $row, $result)) {
                $rows[$i] = $row;
            }
        }

        return new QueryResult($rows, $result->columns);
    }

    /**
     * @param ?array<int, array{expression: \Vimeo\MysqlEngine\Query\Expression\Expression, direction: string}> $orders
     */
    protected static function applyOrderBy(
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        ?array $orders,
        QueryResult $result
    ) : QueryResult {
        if (!$orders) {
            return $result;
        }

        // allow all column expressions to fall through to the full row
        foreach ($orders as $rule) {
            $expr = $rule['expression'];

            if ($expr instanceof ColumnExpression
                && !$expr->tableName
            ) {
                $expr->allowFallthrough();
            }
        }

        $sort_fun = function (array $a, array $b) use ($conn, $scope, $orders, $result): int {
            foreach ($orders as $rule) {
                $value_a = Expression\Evaluator::evaluate($conn, $scope, $rule['expression'], $a, $result);
                $value_b = Expression\Evaluator::evaluate($conn, $scope, $rule['expression'], $b, $result);

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

        $rows = $result->rows;

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

        return new QueryResult(array_values($rows), $result->columns);
    }

    protected static function applyLimit(?LimitClause $limit, Scope $scope, QueryResult $result) : QueryResult
    {
        if ($limit === null) {
            return $result;
        }

        if ($limit->offset === null) {
            $offset = 0;
        } elseif ($limit->offset instanceof ConstantExpression) {
            $offset = (int) $limit->offset->value;
        } elseif ($limit->offset instanceof NamedPlaceholderExpression) {
            $offset = (int) Expression\NamedPlaceholderEvaluator::evaluate($scope, $limit->offset);
        } else {
            $offset = (int) Expression\QuestionMarkPlaceholderEvaluator::evaluate($scope, $limit->offset);
        }

        if ($limit->rowcount instanceof ConstantExpression) {
            $rowcount = (int) $limit->rowcount->value;
        } elseif ($limit->rowcount instanceof NamedPlaceholderExpression) {
            $rowcount = (int) Expression\NamedPlaceholderEvaluator::evaluate($scope, $limit->rowcount);
        } else {
            $rowcount = (int) Expression\QuestionMarkPlaceholderEvaluator::evaluate($scope, $limit->rowcount);
        }

        return new QueryResult(
            \array_slice($result->rows, $offset, $rowcount, true),
            $result->columns
        );
    }

    /**
     * @return array{0:string, 1:string}
     */
    public static function parseTableName(\Vimeo\MysqlEngine\FakePdoInterface $conn, string $table)
    {
        if (\strpos($table, '.')) {
            $parts = \explode('.', $table);
            if (\count($parts) !== 2) {
                throw new ProcessorException("Table name {$table} has too many parts");
            }
            list($database, $table_name) = $parts;
            return [$database, $table_name];
        }

        $database = $conn->getDatabaseName();
        return [$database, $table];
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
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
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
            if (!$expression->left instanceof ColumnExpression || $expression->right === null) {
                throw new \TypeError('Failed assertion');
            }

            $column = $expression->left->columnName;

            if (!isset($valid_fields[$column])) {
                throw new ProcessorException("Invalid update column {$column}");
            }

            $set_clauses[] = ['column' => $column, 'expression' => $expression->right];
        }

        $update_count = 0;

        $last_insert_id = null;

        $original_result = new QueryResult($original_table, $table_definition->columns);

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
                        $original_result
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
                    $original_result
                );
            }

            $row = DataIntegrity::coerceToSchema($conn, $row, $table_definition);

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

        $conn->setLastInsertId((string) $last_insert_id);

        return [$update_count, $original_table];
    }
}

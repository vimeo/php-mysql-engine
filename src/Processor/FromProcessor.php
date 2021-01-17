<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\FromClause;
use Vimeo\MysqlEngine\Schema\Column;

final class FromProcessor
{
    /**
     * @return array{array<int, array<string, mixed>>, array<string, Column>}
     */
    public static function process(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, FromClause $stmt)
    {
        $rows = [];
        $columns = [];
        $is_first_table = true;
        $left_column_list = [];

        foreach ($stmt->tables as $table) {
            if (\array_key_exists('subquery', $table)) {
                [$res, $subquery_columns] = \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
                    $conn,
                    $scope,
                    $table['subquery']->query
                );

                $table_columns = [];

                foreach ($subquery_columns as $column_name => $column) {
                    $parts = \explode('.%.', $column_name);
                    $table_columns[\end($parts)] = $column;
                }

                $name = $table['name'];
            } else {
                $table_name = $table['name'];
                list($database, $table_name) = Processor::parseTableName($conn, $table_name);
                $name = $table['alias'] ?? $table_name;
                $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

                if ($table_definition === null) {
                    throw new SQLFakeRuntimeException(
                        "Table {$table_name} not found in schema and strict mode is enabled"
                    );
                }

                $table_columns = $table_definition->columns;

                $res = $conn->getServer()->getTable($database, $table_name);

                if ($res === null) {
                    $res = [];
                }
            }

            $new_columns = [];

            foreach ($table_columns as $column_name => $column) {
                $new_columns[$name . '.%.' . $column_name] = $column;
            }

            $new_dataset = [];

            $ordered_fields = array_keys($table_columns);

            foreach ($res as $row) {
                $m = [];
                foreach ($ordered_fields as $field) {
                    if (!\array_key_exists($field, $row)) {
                        continue;
                    }
                    $m["{$name}.%.{$field}"] = $row[$field];
                }
                $new_dataset[] = $m;
            }

            if ($rows || !$is_first_table) {
                [$rows, $columns] = JoinProcessor::process(
                    $conn,
                    $scope,
                    [$rows, $columns],
                    [$new_dataset, $new_columns],
                    $name,
                    $table['join_type'],
                    $table['join_operator'] ?? null,
                    $table['join_expression'] ?? null
                );
            } else {
                $rows = $new_dataset;
                $columns = array_merge($columns, $new_columns);
            }

            if ($is_first_table) {
                //Metrics::trackQuery(QueryType::SELECT, $conn->getServer()->name, $name, $sql);
                $is_first_table = false;
            }
        }

        return [$rows, $columns];
    }
}

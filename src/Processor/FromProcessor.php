<?php
namespace MysqlEngine\Processor;

use MysqlEngine\Query\FromClause;
use MysqlEngine\Schema\Column;

final class FromProcessor
{
    public static function process(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        FromClause $stmt
    ) : QueryResult {

        $result = null;

        if (!$stmt->tables) {
            throw new ProcessorException('select tables should not be empty');
        }

        foreach ($stmt->tables as $table) {
            if (\array_key_exists('subquery', $table)) {
                $subquery_result = \MysqlEngine\Processor\SelectProcessor::process(
                    $conn,
                    $scope,
                    $table['subquery']->query
                );

                $res = $subquery_result->rows;

                $table_columns = [];

                foreach ($subquery_result->columns as $column_name => $column) {
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
                    throw new ProcessorException(
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

            $new_result = new QueryResult($new_dataset, $new_columns);

            if ($result) {
                $result = JoinProcessor::process(
                    $conn,
                    $scope,
                    $result,
                    $new_result,
                    $name,
                    $table['join_type'],
                    $table['join_operator'] ?? null,
                    $table['join_expression'] ?? null
                );
            } else {
                $result = $new_result;
            }
        }

        return $result;
    }
}

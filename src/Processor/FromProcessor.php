<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\FromClause;

final class FromProcessor
{
    /**
     * @return array<int, array<string, mixed>>
     */
    public static function process(\Vimeo\MysqlEngine\FakePdo $conn, FromClause $stmt)
    {
        $data = [];
        $columns = [];
        $is_first_table = true;
        $left_column_list = [];

        foreach ($stmt->tables as $table) {
            $table_definition = null;
            if (\array_key_exists('subquery', $table)) {
                $res = Expression\Evaluator::evaluate($table['subquery'], [], $conn);
                $name = $table['name'];
            } else {
                $table_name = $table['name'];
                list($database, $table_name) = Processor::parseTableName($conn, $table_name);
                $name = $table['alias'] ?? $table_name;
                $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

                if ($table_definition === null) {
                    throw new SQLFakeRuntimeException("Table {$table_name} not found in schema and strict mode is enabled");
                }

                $res = $conn->getServer()->getTable($database, $table_name);

                if ($res === null) {
                    $res = [];
                }
            }

            $new_dataset = [];

            if ($table_definition !== null) {
                $ordered_fields = array_keys($table_definition->columns);
                foreach ($res as $row) {
                    $m = [];
                    foreach ($ordered_fields as $field) {
                        if (!\array_key_exists($field, $row)) {
                            continue;
                        }
                        $m["{$name}.{$field}"] = $row[$field];
                    }
                    $new_dataset[] = $m;
                }
            } else {
                foreach ($res as $row) {
                    $m = [];
                    foreach ($row as $key => $val) {
                        $m["{$name}.{$key}"] = $val;
                    }
                    $new_dataset[] = $m;
                }
            }

            if ($data || !$is_first_table) {
                $data = JoinProcessor::process(
                    $conn,
                    $data,
                    $new_dataset,
                    $name,
                    $table['join_type'],
                    $table['join_operator'] ?? null,
                    $table['join_expression'] ?? null,
                    $table_definition
                );
            } else {
                $data = $new_dataset;
            }

            if ($is_first_table) {
                //Metrics::trackQuery(QueryType::SELECT, $conn->getServer()->name, $name, $sql);
                $is_first_table = false;
            }
        }

        return $data;
    }
}

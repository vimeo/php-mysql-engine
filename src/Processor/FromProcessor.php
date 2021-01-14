<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Query\FromClause;

final class FromProcessor
{
    /**
     * @return array<int, array<string, mixed>>
     */
    public static function process(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, FromClause $stmt)
    {
        $data = [];
        $columns = [];
        $is_first_table = true;
        $left_column_list = [];

        foreach ($stmt->tables as $table) {
            if (\array_key_exists('subquery', $table)) {
                $res = Expression\Evaluator::evaluate($conn, $scope, $table['subquery'], []);

                $columns = [];

                foreach ($table['subquery']->query->selectExpressions as $expr) {
                    // the varchar here is irrelevant – we just want the column to exist
                    $columns[$expr->name] = new \Vimeo\MysqlEngine\Schema\Column\Varchar(10);
                }

                $name = $table['name'];
                $table_definition = new \Vimeo\MysqlEngine\Schema\TableDefinition($name, '', $columns);
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

                $res = $conn->getServer()->getTable($database, $table_name);

                if ($res === null) {
                    $res = [];
                }
            }

            $new_dataset = [];

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

            if ($data || !$is_first_table) {
                $data = JoinProcessor::process(
                    $conn,
                    $scope,
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

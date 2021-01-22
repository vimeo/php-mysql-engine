<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataIntegrity;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\InsertQuery;
use Vimeo\MysqlEngine\Schema\Column\IntegerColumn;

final class InsertProcessor extends Processor
{
    public static function process(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        InsertQuery $stmt
    ) : int {
        list($database, $table_name) = self::parseTableName($conn, $stmt->table);

        $table = $conn->getServer()->getTable($database, $table_name) ?? [];
        //Metrics::trackQuery(QueryType::INSERT, $conn->getServer()->name, $table_name, $this->sql);
        $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

        if ($table_definition === null) {
            throw new ProcessorException("Table {$table_name} not found in schema");
        }

        $rows_affected = 0;

        $row = [];

        $last_insert_id = null;

        $conn->lastInsertId = "0";

        foreach ($stmt->values as $value_list) {
            $row = [];

            foreach ($stmt->insertColumns as $key => $col) {
                $row[$col] = Expression\Evaluator::evaluate(
                    $conn,
                    $scope,
                    $value_list[$key],
                    [],
                    new QueryResult([], [])
                );
            }

            $row = DataIntegrity::coerceToSchema($conn, $row, $table_definition);

            $unique_key_violation = DataIntegrity::checkUniqueConstraints($table, $row, $table_definition);

            if ($unique_key_violation !== null) {
                list($msg, $row_id) = $unique_key_violation;
                if ($stmt->updateExpressions) {
                    $existing_row = $table[$row_id];
                    list($affected, $table) = self::applySet(
                        $conn,
                        $scope,
                        $database,
                        $table_name,
                        [$row_id => $existing_row],
                        $table,
                        $stmt->updateExpressions,
                        $table_definition,
                        $row
                    );
                    $rows_affected += $affected * 2;
                    continue;
                }

                if ($stmt->ignoreDupes) {
                    continue;
                } else {
                    throw new SQLFakeUniqueKeyViolation($msg);
                }
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

            if (\count($table_definition->primaryKeyColumns) === 1 && $conn->lastInsertId === "0") {
                $conn->lastInsertId = (string) $row[$table_definition->primaryKeyColumns[0]];
            }

            $table[] = $row;
            $rows_affected++;
        }

        $conn->getServer()->saveTable($database, $table_name, $table);

        if ($stmt->setClause) {
            list($set_rows_affected) = self::applySet(
                $conn,
                $scope,
                $database,
                $table_name,
                null,
                $table,
                $stmt->setClause,
                $table_definition
            );
        }

        return $rows_affected;
    }
}

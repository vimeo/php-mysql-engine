<?php
namespace MysqlEngine\Processor;

use MysqlEngine\Query\DeleteQuery;

final class DeleteProcessor extends Processor
{
    /**
     * @return int
     */
    public static function process(\MysqlEngine\FakePdoInterface $conn, Scope $scope, DeleteQuery $stmt)
    {
        ($__tmp1__ = $stmt->fromClause) !== null ? $__tmp1__ : (function () {
            throw new \TypeError('Failed assertion');
        })();
        list($database, $table_name) = Processor::parseTableName($conn, $stmt->fromClause['name']);

        $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

        if ($table_definition === null) {
            throw new ProcessorException(
                "Table {$table_name} not found in schema and strict mode is enabled"
            );
        }

        $existing_rows = $conn->getServer()->getTable($database, $table_name) ?? [];
        $columns = $table_definition->columns;
        //Metrics::trackQuery(QueryType::DELETE, $conn->getServer()->name, $table_name, $stmt->sql);

        return self::applyDelete(
            $conn,
            $database,
            $table_name,
            self::applyLimit(
                $stmt->limitClause,
                $scope,
                self::applyOrderBy(
                    $conn,
                    $scope,
                    $stmt->orderBy,
                    self::applyWhere(
                        $conn,
                        $scope,
                        $stmt->whereClause,
                        new QueryResult($existing_rows, $columns)
                    )
                )
            )->rows,
            $existing_rows
        );
    }

    /**
     * @param array<int, array<string, mixed>> $filtered_rows
     * @param array<int, array<string, mixed>> $original_table
     *
     * @return int
     */
    protected static function applyDelete(
        \MysqlEngine\FakePdoInterface $conn,
        string $database,
        string $table_name,
        array $filtered_rows,
        array $original_table
    ) {
        $remaining_rows = \array_filter(
            $original_table,
            function ($row_num) use ($filtered_rows) {
                return !\array_key_exists($row_num, $filtered_rows);
            },
            \ARRAY_FILTER_USE_KEY
        );

        $rows_affected = \count($original_table) - \count($remaining_rows);
        $conn->getServer()->saveTable($database, $table_name, $remaining_rows);
        return $rows_affected;
    }
}

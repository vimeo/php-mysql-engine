<?php
namespace Vimeo\MysqlEngine\Processor;

final class UpdateProcessor extends Processor
{
    public static function process(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        \Vimeo\MysqlEngine\Query\UpdateQuery $stmt
    ) : int {
        list($table_name, $database) = self::processUpdateClause($conn, $stmt);

        $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

        $existing_rows = $conn->getServer()->getTable($database, $table_name) ?: [];

        //Metrics::trackQuery(QueryType::UPDATE, $conn->getServer()->name, $table_name, $this->sql);

        $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

        list($rows_affected, $_) = self::applySet(
            $conn,
            $scope,
            $database,
            $table_name,
            self::applyLimit(
                $stmt->limitClause,
                self::applyOrderBy(
                    $conn,
                    $scope,
                    $stmt->orderBy,
                    self::applyWhere(
                        $conn,
                        $scope,
                        $stmt->whereClause,
                        new QueryResult($existing_rows, $table_definition->columns)
                    )
                )
            )->rows,
            $existing_rows,
            $stmt->setClause,
            $table_definition
        );

        return $rows_affected;
    }

    /**
     * @return array{0:string, 1:string}
     */
    protected static function processUpdateClause(
        \Vimeo\MysqlEngine\FakePdo $conn,
        \Vimeo\MysqlEngine\Query\UpdateQuery $stmt
    ) : array {
        list($database, $table_name) = self::parseTableName($conn, $stmt->tableName);
        return [$table_name, $database];
    }
}

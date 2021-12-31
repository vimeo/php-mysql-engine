<?php
namespace MysqlEngine\Processor;

final class UpdateProcessor extends Processor
{
    /**
     * @throws ProcessorException
     * @throws SQLFakeUniqueKeyViolation
     */
    public static function process(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        \MysqlEngine\Query\UpdateQuery $stmt
    ) : int {
        [$table_name, $database] = self::processUpdateClause($conn, $stmt);

        $existing_rows = $conn->getServer()->getTable($database, $table_name) ?: [];

        $table_definition = $conn->getServer()->getTableDefinition($database, $table_name);

        if ($table_definition === null) {
            throw new ProcessorException(
                "Table {$table_name} not found in schema and strict mode is enabled"
            );
        }

        list($rows_affected, $_) = self::applySet(
            $conn,
            $scope,
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
        \MysqlEngine\FakePdoInterface $conn,
        \MysqlEngine\Query\UpdateQuery $stmt
    ) : array {
        [$database, $table_name] = self::parseTableName($conn, $stmt->tableName);
        return [$table_name, $database];
    }
}

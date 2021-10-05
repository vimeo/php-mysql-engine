<?php

namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Query\ShowColumnsQuery;
use Vimeo\MysqlEngine\Query\ShowIndexQuery;
use Vimeo\MysqlEngine\Schema\Column;
use function PHPUnit\Framework\assertIsArray;

class ShowColumnsProcessor extends Processor
{
    public static function process(
        FakePdoInterface $conn,
        Scope $scope,
        ShowColumnsQuery $stmt
    ): QueryResult {
        [$database, $table] = Processor::parseTableName($conn, $stmt->table);
        $table_definition = $conn->getServer()->getTableDefinition(
            $database,
            $table
        );
        if (!$table_definition) {
            return new QueryResult([], []);
        }
        $columns = [
            'Field' => new Column\Varchar(255),
            'Type' => new Column\Varchar(255),
            'Collation' => new Column\Varchar(255),
            'Null' => new Column\Enum(['NO', 'YES']),
            'Key' => new Column\Enum(['PRI']),
            'Default' => new Column\Varchar(255),
            'Extra' => new Column\Enum(['auto_increment']),
            'Privilegies' => new Column\Varchar(255),
            'Comment' => new Column\Varchar(255),
        ];
        $rows = [];
        foreach ($table_definition->columns as $name => $column) {
            $rows[] = [
                'Field' => $name,
                'Type' => self::resolveType($column),
                'Collation' => self::resolveCollation($column),
                'Null' => $column->isNullable() ? 'YES' : 'NO',
                'Key' => in_array($name, $table_definition->primaryKeyColumns) ? 'PRI' : '',
                'Default' => $column->getDefault(),
                'Extra' => self::resolveExtra($column),
                'Privilegies' => 'select,insert,update,references',
                'Comment' => '',
            ];
        }
        $result = self::applyWhere($conn, $scope, $stmt->whereClause, new QueryResult($rows, $columns));

        $rows = array_merge($result->rows);
        $columns = $result->columns;
        if (!$stmt->isFull) {
            $allowedColumns = [
                'Field',
                'Type',
                'Null',
                'Key',
                'Default',
                'Extra',
            ];
            $columns = array_intersect_key($columns, array_flip($allowedColumns));
        }

        return new QueryResult(array_merge($result->rows), $result->columns);
    }

    private static function resolveType(Column $column): string
    {
        if ($column instanceof Column\Varchar) {
            $type = 'varchar(255)';
        } elseif ($column instanceof Column\IntColumn) {
            $type = 'int(11)';
        } elseif ($column instanceof Column\DateTime) {
            $type = 'datetime';
        } else {
            throw new \UnexpectedValueException('Column type not specified.');
        }

        return $type;
    }

    private static function resolveCollation(Column $column): string
    {
        if (is_subclass_of($column, Column\CharacterColumn::class)) {
            $collation = $column->getCollation();
        }

        return $collation ?? '';
    }

    private static function resolveDefault(Column $column): ?string
    {
        if ($column instanceof Column\Defaultable) {
            $default = $column->getDefault();
        }

        return $default ?? null;
    }

    private static function resolveExtra(Column $column): string
    {
        if ($column instanceof Column\IntegerColumn) {
            $extra = $column->isAutoIncrement() ? 'auto_increment' : '';
        }

        return $extra ?? '';
    }
}

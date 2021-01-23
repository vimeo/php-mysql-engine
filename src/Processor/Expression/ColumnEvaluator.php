<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Schema\Column;

final class ColumnEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return scalar|null
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        ColumnExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if ($expr->name === '*') {
            return 1;
        }

        $first_row = reset($row);
        $row = \is_array($first_row) ? $first_row : $row;

        if (\array_key_exists($expr->columnExpression, $row)) {
            return $row[$expr->columnExpression];
        }

        if (($expr->tableName === null && $expr->columnName !== null) || $expr->allowFallthrough) {
            foreach ($row as $key => $col) {
                $key_col = \preg_replace('/.*\.%\./', '', $key);

                if ($expr->columnName === $key_col) {
                    return $col;
                }
            }
        }

        if (\array_key_exists($expr->name, $row)) {
            return $row[$expr->name];
        }

        if (\array_key_exists($expr->tableName . '.%.' . $expr->columnName, $row)) {
            return $row[$expr->tableName . '.%.' . $expr->columnName];
        }

        throw new ProcessorException(
            'Column with index ' . $expr->columnExpression . ' not found in row at offset ' . $expr->start
        );
    }

    /**
     * @param  array<string, Column> $columns
     * @return Column
     */
    public static function getColumnSchema(
        ColumnExpression $expr,
        array $columns
    ) : Column {
        if ($expr->name === '*') {
            throw new ProcessorException(
                'Column with index ' . $expr->name . ' not found in row'
            );
        }

        if (\array_key_exists($expr->columnExpression, $columns)) {
            return $columns[$expr->columnExpression];
        }

        if (($expr->tableName === null && $expr->columnName !== null) || $expr->allowFallthrough) {
            foreach ($columns as $key => $col) {
                $key_col = \preg_replace('/.*\.%\./', '', $key);

                if ($expr->columnName === $key_col) {
                    return $col;
                }
            }
        }

        if (\array_key_exists($expr->name, $columns)) {
            return $columns[$expr->name];
        }

        if (\array_key_exists($expr->tableName . '.%.' . $expr->columnName, $columns)) {
            return $columns[$expr->tableName . '.%.' . $expr->columnName];
        }

        throw new ProcessorException(
            'Column with index ' . $expr->columnExpression . ' not found in row'
        );
    }
}

<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class ColumnEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return scalar|null
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, ColumnExpression $expr, array $row)
    {
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
                $parts = \explode('.', $key);
                if (\end($parts) === $expr->columnName) {
                    return $col;
                }
            }
        }

        if (\array_key_exists($expr->name, $row)) {
            return $row[$expr->name];
        }

        if (\array_key_exists($expr->tableName . '.' . $expr->columnName, $row)) {
            return $row[$expr->tableName . '.' . $expr->columnName];
        }

        throw new SQLFakeRuntimeException(
            'Column with index ' . $expr->columnExpression . ' not found in row'
        );
    }
}

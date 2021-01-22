<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;

final class VariableEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        VariableExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if (\array_key_exists($expr->variableName, $scope->variables)) {
            return $scope->variables[$expr->variableName];
        }

        return null;
    }
}

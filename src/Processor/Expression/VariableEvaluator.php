<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Query\Expression\VariableExpression;
use MysqlEngine\Processor\Scope;

final class VariableEvaluator
{
    /**
     * @return mixed
     */
    public static function evaluate(Scope $scope, VariableExpression $expr)
    {
        if (\array_key_exists($expr->variableName, $scope->variables)) {
            return $scope->variables[$expr->variableName];
        }

        return null;
    }
}

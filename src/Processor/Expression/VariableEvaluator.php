<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\Processor\Scope;

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

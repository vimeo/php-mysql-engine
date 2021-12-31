<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\NamedPlaceholderExpression;
use MysqlEngine\Processor\Scope;

final class NamedPlaceholderEvaluator
{
    /**
     * @return mixed
     */
    public static function evaluate(Scope $scope, NamedPlaceholderExpression $expr)
    {
        if (\array_key_exists($expr->parameterName, $scope->parameters)) {
            return $scope->parameters[$expr->parameterName];
        }

        throw new ProcessorException('Parameter offset ' . $expr->parameterName . ' out of range');
    }
}

<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\ParameterExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class ParameterEvaluator
{
    /**
     * @return mixed
     */
    public static function evaluate(Scope $scope, ParameterExpression $expr)
    {
        if (\array_key_exists($expr->parameterName, $scope->parameters)) {
            return $scope->parameters[$expr->parameterName];
        }

        throw new ProcessorException('Parameter offset ' . $expr->parameterName . ' out of range');
    }
}



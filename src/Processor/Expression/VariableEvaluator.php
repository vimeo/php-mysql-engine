<?php

namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class VariableEvaluator
{
    /**
     * @return mixed
     * @throws ProcessorException
     */
    public static function evaluate(Scope $scope, VariableExpression $expr)
    {
        if (strpos($expr->variableName, '@') === 0) {
            return self::getSystemVariable(substr($expr->variableName, 1));
        }

        if (\array_key_exists($expr->variableName, $scope->variables)) {
            return $scope->variables[$expr->variableName];
        }

        return null;
    }

    /**
     * @param string $variableName
     *
     * @return string
     * @throws ProcessorException
     */
    private static function getSystemVariable(string $variableName): string
    {
        switch ($variableName) {
            case 'session.time_zone':
                return date_default_timezone_get();
            default:
                throw new ProcessorException("System variable $variableName is not supported yet!");
        }
    }
}

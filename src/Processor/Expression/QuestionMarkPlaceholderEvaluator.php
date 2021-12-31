<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\QuestionMarkPlaceholderExpression;
use MysqlEngine\Processor\Scope;

final class QuestionMarkPlaceholderEvaluator
{
    /**
     * @return mixed
     */
    public static function evaluate(Scope $scope, QuestionMarkPlaceholderExpression $expr)
    {
        if (\array_key_exists($expr->offset, $scope->parameters)) {
            return $scope->parameters[$expr->offset];
        }

        throw new ProcessorException('Parameter offset ' . $expr->offset . ' out of range');
    }
}

<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\UnaryExpression;
use Vimeo\MysqlEngine\Processor\Scope;

final class UnaryEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(\Vimeo\MysqlEngine\FakePdo $conn, Scope $scope, UnaryExpression $expr, array $row)
    {
        if ($expr->subject === null) {
            throw new SQLFakeRuntimeException("Attempted to evaluate unary operation with no operand");
        }

        $val = Evaluator::evaluate($conn, $scope, $expr->subject, $row);

        switch ($expr->operator) {
            case 'UNARY_MINUS':
                return -1 * (double) $val;
            case 'UNARY_PLUS':
                return (double) $val;
            case '~':
                return ~(int) $val;
            case '!':
                return (int) !$val;
            default:
                throw new SQLFakeRuntimeException("Unimplemented unary operand {$expr->name}");
        }

        return $val;
    }
}

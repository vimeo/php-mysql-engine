<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\UnaryExpression;

final class UnaryEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(UnaryExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        if ($expr->subject === null) {
            throw new SQLFakeRuntimeException("Attempted to evaluate unary operation with no operand");
        }

        $val = Evaluator::evaluate($expr->subject, $row, $conn);

        switch ($expr->operator) {
            case 'UNARY_MINUS':
                return -1 * (double) $val;
            case 'UNARY_PLUS':
                return (double) $val;
            case '~':
                return ~(int) $val;
            default:
                throw new SQLFakeRuntimeException("Unimplemented unary operand {$expr->name}");
        }

        return $val;
    }
}

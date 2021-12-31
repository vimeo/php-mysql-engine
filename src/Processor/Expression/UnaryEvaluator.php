<?php
namespace MysqlEngine\Processor\Expression;

use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\Query\Expression\UnaryExpression;
use MysqlEngine\Processor\QueryResult;
use MysqlEngine\Processor\Scope;
use MysqlEngine\Schema\Column;

final class UnaryEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(
        \MysqlEngine\FakePdoInterface $conn,
        Scope $scope,
        UnaryExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if ($expr->subject === null) {
            throw new ProcessorException("Attempted to evaluate unary operation with no operand");
        }

        $val = Evaluator::evaluate($conn, $scope, $expr->subject, $row, $result);

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
                throw new ProcessorException("Unimplemented unary operand {$expr->name}");
        }
    }
}

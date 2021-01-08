<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\TokenType;

final class BinaryOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     */
    public static function evaluate(
        BinaryOperatorExpression $expr,
        array $row,
        \Vimeo\MysqlEngine\FakePdo $conn
    ) {
        $right = $expr->right;
        $left = $expr->left;

        if ($left instanceof RowExpression) {
            if (!$right instanceof RowExpression) {
                throw new SQLFakeRuntimeException("Expected row expression on RHS of {$expr->operator} operand");
            }

            return (int) self::evaluateRowComparison($expr, $left, $right, $row, $conn);
        }

        if ($right === null) {
            throw new SQLFakeRuntimeException("Attempted to evaluate BinaryOperatorExpression with no right operand");
        }

        if ($right instanceof IntervalOperatorExpression
            && ($expr->operator === '+' || $expr->operator === '-')
        ) {
            $functionName = $expr->operator === '+' ? 'DATE_ADD' : 'DATE_SUB';

            return FunctionEvaluator::evaluate(
                new FunctionExpression(
                    new \Vimeo\MysqlEngine\Parser\Token(
                        TokenType::SQLFUNCTION,
                        $functionName,
                        $functionName
                    ),
                    [
                        $left,
                        $right,
                    ],
                    false
                ),
                $row,
                $conn
            );
        }

        $l_value = Evaluator::evaluate($left, $row, $conn);
        $r_value = Evaluator::evaluate($right, $row, $conn);
        $as_string = $left->getType() == TokenType::STRING_CONSTANT || $right->getType() == TokenType::STRING_CONSTANT;

        switch ($expr->operator) {
            case '':
                throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');

            case 'AND':
                if ((bool) $l_value && (bool) $r_value) {
                    return (int) (!$expr->negated);
                }
                return (int) $expr->negated;

            case 'OR':
                if ((bool) $l_value || (bool) $r_value) {
                    return (int) (!$expr->negated);
                }
                return (int) $expr->negated;

            case '=':
                return $l_value == $r_value ? 1 : 0 ^ $expr->negatedInt;

            case '<>':
            case '!=':
                if ($as_string) {
                    return (string) $l_value != (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

                return (int) $l_value != (int) $r_value ? 1 : 0 ^ $expr->negatedInt;

            case '>':
                if ($as_string) {
                    return (string) $l_value > (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

                return (int) $l_value > (int) $r_value ? 1 : 0 ^ $expr->negatedInt;
                // no break
            case '>=':
                if ($as_string) {
                    return (string) $l_value >= (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

                return (int) $l_value >= (int) $r_value ? 1 : 0 ^ $expr->negatedInt;

            case '<':
                if ($as_string) {
                    return (string) $l_value < (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

                return (int) $l_value < (int) $r_value ? 1 : 0 ^ $expr->negatedInt;

            case '<=':
                if ($as_string) {
                    return (string) $l_value <= (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

                return (int) $l_value <= (int) $r_value ? 1 : 0 ^ $expr->negatedInt;

            case '*':
            case '%':
            case 'MOD':
            case '-':
            case '+':
            case '<<':
            case '>>':
            case '/':
            case 'DIV':
            case '|':
            case '&':
                $left_number = self::extractNumericValue($l_value);
                $right_number = self::extractNumericValue($r_value);

                switch ($expr->operator) {
                    case '*':
                        return $left_number * $right_number;
                    case '%':
                    case 'MOD':
                        return \fmod((double) $left_number, (double) $right_number);
                    case '/':
                        return $left_number / $right_number;
                    case 'DIV':
                        return (int) ($left_number / $right_number);
                    case '-':
                        return $left_number - $right_number;
                    case '+':
                        return $left_number + $right_number;
                    case '<<':
                        return (int) $left_number << (int) $right_number;
                    case '>>':
                        return (int) $left_number >> (int) $right_number;
                    case '|':
                        return (int) $left_number | (int) $right_number;
                    case '&':
                        return (int) $left_number & (int) $right_number;
                }

                throw new SQLFakeRuntimeException("Operator recognized but not implemented");

            case 'LIKE':
                $left_string = (string) Evaluator::evaluate($left, $row, $conn);

                if (!$right instanceof ConstantExpression) {
                    throw new SQLFakeRuntimeException("LIKE pattern should be a constant string");
                }

                $pattern = (string) $r_value;
                $start_pattern = '^';
                $end_pattern = '$';

                if ($pattern[0] === '%') {
                    $start_pattern = '';
                    $pattern = \substr($pattern, 1);
                }

                if (\substr($pattern, -1) === '%') {
                    $end_pattern = '';
                    $pattern = \substr($pattern, 0, -1);
                }

                // escape all + characters
                $pattern = \preg_quote($pattern, '/');
                $pattern = \preg_replace('/(?<!\\\\)%/', '.*?', $pattern);
                $pattern = \preg_replace('/(?<!\\\\)_/', '.', $pattern);
                $regex = '/' . $start_pattern . $pattern . $end_pattern . '/s';

                return ((bool) \preg_match($regex, $left_string) ? 1 : 0) ^ $expr->negatedInt;

            case 'IS':
                if (!$right instanceof ConstantExpression) {
                    throw new SQLFakeRuntimeException("Unsupported right operand for IS keyword");
                }
                $val = Evaluator::evaluate($left, $row, $conn);
                $r = $r_value;
                if ($r === null) {
                    return ($val === null ? 1 : 0) ^ $expr->negatedInt;
                }
                throw new SQLFakeRuntimeException("Unsupported right operand for IS keyword");

            case 'RLIKE':
            case 'REGEXP':
                $left_string = (string) Evaluator::evaluate($left, $row, $conn);
                $case_insensitive = 'i';
                if ($right instanceof FunctionExpression && $right->functionName() == 'BINARY') {
                    $case_insensitive = '';
                }
                $pattern = (string) $r_value;
                $regex = '/' . $pattern . '/' . $case_insensitive;
                return ((bool) \preg_match($regex, $left_string) ? 1 : 0) ^ $expr->negatedInt;

            case '&&':
            case 'BINARY':
            case 'COLLATE':
            case '^':
            case '<=>':
            case '||':
            case 'XOR':
            case 'SOUNDS':
            case 'ANY':
            case 'SOME':
            default:
                throw new SQLFakeRuntimeException("Operator {$expr->operator} not implemented in SQLFake");
        }
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return bool
     */
    private static function evaluateRowComparison(
        BinaryOperatorExpression $expr,
        RowExpression $left,
        RowExpression $right,
        array $row,
        \Vimeo\MysqlEngine\FakePdo $conn
    ) {
        $left_elems = Evaluator::evaluate($left, $row, $conn);
        assert(\is_array($left_elems), "RowExpression must return vec");
        $right_elems = Evaluator::evaluate($right, $row, $conn);
        assert(\is_array($right_elems), "RowExpression must return vec");
        if (\count($left_elems) !== \count($right_elems)) {
            throw new SQLFakeRuntimeException("Mismatched column count in row comparison expression");
        }
        $last_index = \array_key_last($left_elems);
        $match = true;
        foreach ($left_elems as $index => $le) {
            $re = $right_elems[$index];
            if ($le == $re && $index !== $last_index) {
                continue;
            }
            switch ($expr->operator) {
                case '=':
                    return $le == $re;
                case '<>':
                case '!=':
                    return $le != $re;
                case '>':
                    return $le > $re;
                case '>=':
                    return $le >= $re;
                case '<':
                    return $le < $re;
                case '<=':
                    return $le <= $re;
                default:
                    throw new SQLFakeRuntimeException("Operand {$expr->operator} should contain 1 column(s)");
            }
        }
        return false;
    }

    /**
     * @param scalar|array<scalar> $val
     *
     * @return numeric
     */
    protected static function extractNumericValue($val)
    {
        if (\is_array($val)) {
            if (0 === \count($val)) {
                $val = 0;
            } else {
                $val = self::extractNumericValue(reset($val));
            }
        }

        return \strpos((string) $val, '.') !== false ? (double) $val : (int) $val;
    }
}

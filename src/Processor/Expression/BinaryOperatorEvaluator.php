<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;

final class BinaryOperatorEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     */
    public static function evaluate(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        BinaryOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $right = $expr->right;
        $left = $expr->left;

        if ($left instanceof RowExpression) {
            if (!$right instanceof RowExpression) {
                throw new ProcessorException("Expected row expression on RHS of {$expr->operator} operand");
            }

            return (int) self::evaluateRowComparison($conn, $scope, $expr, $left, $right, $row, $result);
        }

        if ($right === null) {
            throw new ProcessorException("Attempted to evaluate BinaryOperatorExpression with no right operand");
        }

        if ($expr->operator === 'COLLATE') {
            $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);

            return $l_value;
        }

        if ($right instanceof IntervalOperatorExpression
            && ($expr->operator === '+' || $expr->operator === '-')
        ) {
            $functionName = $expr->operator === '+' ? 'DATE_ADD' : 'DATE_SUB';

            return FunctionEvaluator::evaluate(
                $conn,
                $scope,
                new FunctionExpression(
                    new \Vimeo\MysqlEngine\Parser\Token(
                        TokenType::SQLFUNCTION,
                        $functionName,
                        $functionName,
                        $expr->start
                    ),
                    [
                        $left,
                        $right,
                    ],
                    false
                ),
                $row,
                $result
            );
        }

        switch ($expr->operator) {
            case '':
                throw new ProcessorException('Attempted to evaluate BinaryOperatorExpression with empty operator');

            case 'AND':
                $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);


                if ($l_value) {
                    $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                    if ($r_value) {
                        return (int) (!$expr->negated);
                    }
                }

                return (int) $expr->negated;

            case 'OR':
                $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);

                if ($l_value) {
                    return (int) (!$expr->negated);
                }

                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                if ($r_value) {
                    return (int) (!$expr->negated);
                }

                return (int) $expr->negated;

            case '=':
            case '<>':
            case '!=':
            case '>':
            case '>=':
            case '<':
            case '<=':
                $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);
                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                $as_string = false;

                $l_type = Evaluator::getColumnSchema($left, $scope, $result->columns);
                $r_type = Evaluator::getColumnSchema($right, $scope, $result->columns);

                if ($l_type->getPhpType() === 'string' && $r_type->getPhpType() === 'string') {
                    if (\preg_match('/^[0-9]{2,4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$/', $l_value)
                        && \preg_match('/^[0-9]{2,4}-[0-1][0-9]-[0-3][0-9]$/', $r_value)
                    ) {
                        $r_value .= ' 00:00:00';
                    } elseif (\preg_match('/^[0-9]{2,4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$/', $r_value)
                        && \preg_match('/^[0-9]{2,4}-[0-1][0-9]-[0-3][0-9]$/', $l_value)
                    ) {
                        $l_value .= ' 00:00:00';
                    }

                    $as_string = true;
                }

                if ($l_value === null || $r_value === null) {
                    return null;
                }

                switch ($expr->operator) {
                    case '=':
                        if ($as_string) {
                            return \strtolower((string) $l_value) === \strtolower((string) $r_value) ? 1 : 0 ^ $expr->negatedInt;
                        }

                        if (empty($l_value) && empty($r_value)) {
                            return !$expr->negatedInt;
                        }

                        return $l_value == $r_value ? 1 : 0 ^ $expr->negatedInt;

                    case '<>':
                    case '!=':
                        if ($as_string) {
                            return \strtolower((string) $l_value) !== \strtolower((string) $r_value) ? 1 : 0 ^ $expr->negatedInt;
                        }

                        if (empty($l_value) && empty($r_value)) {
                            return $expr->negatedInt;
                        }

                        return $l_value != $r_value ? 1 : 0 ^ $expr->negatedInt;

                    case '>':
                        if ($as_string) {
                            return (string) $l_value > (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                        }

                        return (float) $l_value > (float) $r_value ? 1 : 0 ^ $expr->negatedInt;
                        // no break
                    case '>=':
                        if ($as_string) {
                            return (string) $l_value >= (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                        }

                        return (float) $l_value >= (float) $r_value ? 1 : 0 ^ $expr->negatedInt;

                    case '<':
                        if ($as_string) {
                            return (string) $l_value < (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                        }

                        return (float) $l_value < (float) $r_value ? 1 : 0 ^ $expr->negatedInt;

                    case '<=':
                        if ($as_string) {
                            return (string) $l_value <= (string) $r_value ? 1 : 0 ^ $expr->negatedInt;
                        }

                        return (float) $l_value <= (float) $r_value ? 1 : 0 ^ $expr->negatedInt;
                }

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
                $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);
                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                if ($l_value === null || $r_value === null) {
                    return null;
                }

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

                throw new ProcessorException("Operator recognized but not implemented");

            case 'LIKE':
                $l_value = Evaluator::evaluate($conn, $scope, $left, $row, $result);
                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                $left_string = (string) Evaluator::evaluate($conn, $scope, $left, $row, $result);

                if ($r_value === null) {
                    return null;
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
                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                if (!$right instanceof ConstantExpression) {
                    throw new ProcessorException("Unsupported right operand for IS keyword");
                }
                $val = Evaluator::evaluate($conn, $scope, $left, $row, $result);
                $r = $r_value;
                if ($r === null) {
                    return ($val === null ? 1 : 0) ^ $expr->negatedInt;
                }
                throw new ProcessorException("Unsupported right operand for IS keyword");

            case 'RLIKE':
            case 'REGEXP':
                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                $left_string = (string) Evaluator::evaluate($conn, $scope, $left, $row, $result);
                $case_insensitive = 'i';
                if ($right instanceof FunctionExpression && $right->functionName() == 'BINARY') {
                    $case_insensitive = '';
                }
                $pattern = (string) $r_value;
                $regex = '/' . $pattern . '/' . $case_insensitive;
                return ((bool) \preg_match($regex, $left_string) ? 1 : 0) ^ $expr->negatedInt;

            case ':=':
                if (!$left instanceof VariableExpression) {
                    throw new ProcessorException("Unsupported left operand for variable assignment");
                }

                $r_value = Evaluator::evaluate($conn, $scope, $right, $row, $result);

                $scope->variables[$left->variableName] = $r_value;

                return $scope->variables[$left->variableName];

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
                throw new ProcessorException("Operator {$expr->operator} not implemented in SQLFake");
        }
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     */
    public static function getColumnSchema(
        BinaryOperatorExpression $expr,
        Scope $scope,
        array $columns
    ) : Column {
        $left = $expr->left;
        $right = $expr->right;

        if ($left instanceof RowExpression) {
            if (!$right instanceof RowExpression) {
                throw new ProcessorException("Expected row expression on RHS of {$expr->operator} operand");
            }

            return new Column\TinyInt(true, 1);
        }

        if ($right === null) {
            throw new ProcessorException("Attempted to evaluate BinaryOperatorExpression with no right operand");
        }

        if ($right instanceof IntervalOperatorExpression
            && ($expr->operator === '+' || $expr->operator === '-')
        ) {
            $functionName = $expr->operator === '+' ? 'DATE_ADD' : 'DATE_SUB';

            return new Column\DateTime();
        }

        if ($expr->operator === 'COLLATE') {
            return new Column\Varchar(255);
        }

        switch ($expr->operator) {
            case '':
                throw new ProcessorException('Attempted to evaluate BinaryOperatorExpression with empty operator');

            case 'AND':
            case 'OR':
            case '=':
            case '<>':
            case '!=':
            case '>':
            case '>=':
            case '<':
            case '<=':
            case 'LIKE':
            case 'IS':
            case 'RLIKE':
            case 'REGEXP':
                return new Column\TinyInt(true, 1);

            case '-':
            case '+':
            case '*':
                $l_type = Evaluator::getColumnSchema($left, $scope, $columns);
                $r_type = Evaluator::getColumnSchema($right, $scope, $columns);

                if ($l_type instanceof Column\IntegerColumn && $r_type instanceof Column\IntegerColumn) {
                    return new Column\IntColumn(false, 11);
                }

                if ($l_type instanceof Column\Decimal && $r_type instanceof Column\Decimal) {
                    return $l_type;
                }

                return new Column\FloatColumn(10, 2);

            case '%':
            case 'MOD':
                $l_type = Evaluator::getColumnSchema($left, $scope, $columns);

                if ($l_type instanceof Column\IntegerColumn) {
                    return new Column\IntColumn(true, 11);
                }

                return new Column\FloatColumn(10, 2);

            case 'DIV':
                return new Column\IntColumn(false, 11);

            case '/':
                return new Column\FloatColumn(10, 2);

            case '<<':
            case '>>':
            case '|':
            case '&':
                return new Column\IntColumn(false, 11);

            case ':=':
                $r_type = Evaluator::getColumnSchema($right, $scope, $columns);

                return $r_type;
        }

        return new Column\Varchar(255);
    }

    /**
     * @param mixed $data
     *
     * @return mixed
     */
    private static function maybeUnrollGroupedDataset($data)
    {
        if (\is_array($data)) {
            if (!$data) {
                return null;
            }

            if (\count($data) === 1) {
                $data = reset($data);

                if (\is_array($data)) {
                    if (\count($data) === 1) {
                        return reset($data);
                    }

                    throw new ProcessorException("Subquery should return a single column");
                }

                return reset($data);
            }

            throw new ProcessorException("Subquery should return a single column");
        }

        return $data;
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return bool
     */
    private static function evaluateRowComparison(
        \Vimeo\MysqlEngine\FakePdo $conn,
        Scope $scope,
        BinaryOperatorExpression $expr,
        RowExpression $left,
        RowExpression $right,
        array $row,
        QueryResult $result
    ) {
        $left_elems = Evaluator::evaluate($conn, $scope, $left, $row, $result);
        assert(\is_array($left_elems), "RowExpression must return vec");
        $right_elems = Evaluator::evaluate($conn, $scope, $right, $row, $result);
        assert(\is_array($right_elems), "RowExpression must return vec");
        if (\count($left_elems) !== \count($right_elems)) {
            throw new ProcessorException("Mismatched column count in row comparison expression");
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
                    throw new ProcessorException("Operand {$expr->operator} should contain 1 column(s)");
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

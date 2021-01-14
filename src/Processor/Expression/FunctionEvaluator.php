<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\FakePdo;

final class FunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        switch ($expr->functionName) {
            case 'COUNT':
                return self::sqlCount($conn, $scope, $expr, $row);
            case 'SUM':
                return (string) self::sqlSum($conn, $scope, $expr, $row);
            case 'MAX':
                return (string) self::sqlMax($conn, $scope, $expr, $row);
            case 'MIN':
                return (string) self::sqlMin($conn, $scope, $expr, $row);
            case 'MOD':
                return self::sqlMod($conn, $scope, $expr, $row);
            case 'AVG':
                return self::sqlAvg($conn, $scope, $expr, $row);
            case 'IF':
                return self::sqlIf($conn, $scope, $expr, $row);
            case 'IFNULL':
            case 'COALESCE':
                return self::sqlCoalesce($conn, $scope, $expr, $row);
            case 'NULLIF':
                return self::sqlNullif($conn, $scope, $expr, $row);
            case 'SUBSTRING':
            case 'SUBSTR':
                return self::sqlSubstring($conn, $scope, $expr, $row);
            case 'SUBSTRING_INDEX':
                return self::sqlSubstringIndex($conn, $scope, $expr, $row);
            case 'LENGTH':
                return self::sqlLength($conn, $scope, $expr, $row);
            case 'LOWER':
                return self::sqlLower($conn, $scope, $expr, $row);
            case 'UPPER':
                return self::sqlUpper($conn, $scope, $expr, $row);
            case 'CHAR_LENGTH':
            case 'CHARACTER_LENGTH':
                return self::sqlCharLength($conn, $scope, $expr, $row);
            case 'CONCAT_WS':
                return self::sqlConcatWS($conn, $scope, $expr, $row);
            case 'CONCAT':
                return self::sqlConcat($conn, $scope, $expr, $row);
            case 'FIELD':
                return self::sqlColumn($conn, $scope, $expr, $row);
            case 'BINARY':
                return self::sqlBinary($conn, $scope, $expr, $row);
            case 'FROM_UNIXTIME':
                return self::sqlFromUnixtime($conn, $scope, $expr, $row);
            case 'GREATEST':
                return self::sqlGreatest($conn, $scope, $expr, $row);
            case 'VALUES':
                return self::sqlValues($conn, $scope, $expr, $row);
            case 'NOW':
                return \date('Y-m-d H:i:s', time());
            case 'DATE':
                return self::sqlDate($conn, $scope, $expr, $row);
            case 'DATE_FORMAT':
                return self::sqlDateFormat($conn, $scope, $expr, $row);
            case 'ISNULL':
                return self::sqlIsNull($conn, $scope, $expr, $row);
            case 'DATE_SUB':
                return self::sqlDateSub($conn, $scope, $expr, $row);
            case 'DATE_ADD':
                return self::sqlDateAdd($conn, $scope, $expr, $row);
            case 'ROUND':
                return self::sqlRound($conn, $scope, $expr, $row);
        }

        throw new SQLFakeRuntimeException("Function " . $expr->functionName . " not implemented yet");
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return int
     */
    private static function sqlCount(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $rows)
    {
        $inner = $expr->getExpr();

        if ($expr->distinct) {
            $buckets = [];
            foreach ($rows as $row) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();

                $val = Evaluator::evaluate($conn, $scope, $inner, $row);
                if (\is_int($val) || \is_string($val)) {
                    $buckets[$val] = 1;
                }
            }

            return \count($buckets);
        }

        $count = 0;
        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            if (Evaluator::evaluate($conn, $scope, $inner, $row) !== null) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return numeric
     */
    private static function sqlSum(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $rows)
    {
        $expr = $expr->getExpr();

        $column = null;

        if ($expr instanceof ColumnExpression) {
            $server = $conn->getServer();
            $database_name = $expr->databaseName ?: $conn->databaseName;
            $table_name = $expr->tableName;

            if ($database_name
                && $table_name
                && $expr->columnName
                && ($table_definition = $server->getTableDefinition($database_name, $table_name))
            ) {
                $column = $table_definition->columns[$expr->columnName] ?? null;
            }
        }

        $sum = 0;

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $val = Evaluator::evaluate($conn, $scope, $expr, $row);
            $num = \is_int($val) ? $val : (double) $val;
            $sum += $num;
        }

        if ($column) {
            switch ($column->getPhpType()) {
                case 'int':
                    return (int) $sum;

                case 'float':
                    return (float) $sum;

                case 'string':
                    if ($column instanceof \Vimeo\MysqlEngine\Schema\Column\Decimal) {
                        /** @var numeric-string */
                        return \number_format($sum, $column->getDecimalScale(), '.', '');
                    }
            }
        }

        return $sum;
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return mixed
     */
    private static function sqlMin(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $rows)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($conn, $scope, $expr, $row);
        }

        if (0 === \count($values)) {
            return null;
        }

        return \min($values);
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return mixed
     */
    private static function sqlMax(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $rows)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($conn, $scope, $expr, $row);
        }

        if (0 === \count($values)) {
            return null;
        }

        return \max($values);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlMod(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL MOD() function must be called with two arguments");
        }

        $n = $args[0];
        $n_value = (int) Evaluator::evaluate($conn, $scope, $n, $row);
        $m = $args[1];
        $m_value = (int) Evaluator::evaluate($conn, $scope, $m, $row);

        return $n_value % $m_value;
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return mixed
     */
    private static function sqlAvg(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $rows)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $value = Evaluator::evaluate($conn, $scope, $expr, $row);

            if (!\is_int($value) && !\is_float($value)) {
                throw new \TypeError('Failed assertion');
            }
        }

        if (\count($values) === 0) {
            return null;
        }

        return \array_sum($values) / \count($values);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlIf(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL IF() function must be called with three arguments");
        }

        $condition = $args[0];
        $arg_to_evaluate = 2;

        if ((bool) Evaluator::evaluate($conn, $scope, $condition, $row)) {
            $arg_to_evaluate = 1;
        }

        $expr = $args[$arg_to_evaluate];
        return Evaluator::evaluate($conn, $scope, $expr, $row);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlSubstring(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2 && \count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING() function must be called with two or three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);
        $position = $args[1];
        $pos = (int) Evaluator::evaluate($conn, $scope, $position, $row);
        $pos -= 1;
        $length = $args[2] ?? null;

        if ($length !== null) {
            $len = (int) Evaluator::evaluate($conn, $scope, $length, $row);
            return \mb_substr($string, $pos, $len);
        }

        return \mb_substr($string, $pos);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlSubstringIndex(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING_INDEX() function must be called with three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);
        $delimiter = $args[1];
        $delim = (string) Evaluator::evaluate($conn, $scope, $delimiter, $row);
        $pos = $args[2];

        if ($pos !== null) {
            $count = (int) Evaluator::evaluate($conn, $scope, $pos, $row);
            $parts = \explode($delim, $string);

            if ($count < 0) {
                $slice = \array_slice($parts, $count);
            } else {
                $slice = \array_slice($parts, 0, $count);
            }

            return \implode($delim, $slice);
        }

        return '';
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlLower(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LOWER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);
        return \strtolower($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlUpper(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL UPPER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);
        return \strtoupper($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlLength(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);
        return \strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlBinary(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL BINARY() function must be called with one argument");
        }

        $subject = $args[0];
        return Evaluator::evaluate($conn, $scope, $subject, $row);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCharLength(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL CHAR_LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row);

        return \mb_strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCoalesce(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        if (!\count($expr->args)) {
            throw new SQLFakeRuntimeException("MySQL COALESCE() function must be called with at least one argument");
        }

        foreach ($expr->args as $arg) {
            $eval_row = $row;

            if (!$arg->hasAggregate()) {
                $eval_row = self::maybeUnrollGroupedDataset($row);
            }

            $val = Evaluator::evaluate($conn, $scope, $arg, $eval_row);

            if ($val !== null) {
                return $val;
            }
        }

        return null;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlGreatest(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL GREATEST() function must be called with at two arguments");
        }

        $values = [];
        foreach ($expr->args as $arg) {
            $eval_row = $row;

            if (!$arg->hasAggregate()) {
                $eval_row = self::maybeUnrollGroupedDataset($row);
            }

            $val = Evaluator::evaluate($conn, $scope, $arg, $eval_row);
            $values[] = $val;
        }

        return \max($values);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlNullif(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL NULLIF() function must be called with two arguments");
        }

        $left = Evaluator::evaluate($conn, $scope, $args[0], $row);
        $right = Evaluator::evaluate($conn, $scope, $args[1], $row);

        return $left === $right ? null : $left;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlIsNull(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row) : int
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL ISNULL() function must be called with one arguments");
        }

        return Evaluator::evaluate($conn, $scope, $args[0], $row) === null ? 1 : 0;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlFromUnixtime(
        FakePdo $conn,
        Scope $scope, 
        FunctionExpression $expr,
        array $row
    ) : string {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL FROM_UNIXTIME() SQLFake only implemented for 1 argument");
        }

        $column = Evaluator::evaluate($conn, $scope, $args[0], $row);
        $format = 'Y-m-d G:i:s';

        return \date($format, (int) $column);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcat(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT() function must be called with at least two arguments");
        }

        $final_concat = "";
        foreach ($args as $k => $arg) {
            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row);
            $final_concat .= $val;
        }

        return $final_concat;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcatWS(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function must be called with at least two arguments");
        }

        $separator = Evaluator::evaluate($conn, $scope, $args[0], $row);
        if ($separator === null) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function required non null separator");
        }

        $separator = (string) $separator;
        $final_concat = "";

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row);

            if ($final_concat === '') {
                $final_concat = $final_concat . $val;
            } else {
                $final_concat = $final_concat . $separator . $val;
            }
        }

        return $final_concat;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlColumn(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args < 2) {
            throw new SQLFakeRuntimeException("MySQL FIELD() function must be called with at least two arguments");
        }

        $value = Evaluator::evaluate($conn, $scope, $args[0], $row);

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            if ($value == Evaluator::evaluate($conn, $scope, $arg, $row)) {
                return $k;
            }
        }

        return 0;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlValues(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args !== 1) {
            throw new SQLFakeRuntimeException("MySQL VALUES() function must be called with one argument");
        }

        $arg = $args[0];
        if (!$arg instanceof ColumnExpression) {
            throw new SQLFakeRuntimeException("MySQL VALUES() function should be called with a column name");
        }

        if (\substr($arg->columnExpression, 0, 16) !== 'sql_fake_values.') {
            $arg->columnExpression = 'sql_fake_values.' . $arg->columnExpression;
        }

        return Evaluator::evaluate($conn, $scope, $arg, $row);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDate(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row);

        if (strpos($subject, '0000-00-00') === 0) {
            return '0000-00-00';
        }

        return (new \DateTimeImmutable($subject))->format('Y-m-d');
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDateFormat(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_FORMAT() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row);
        $format = Evaluator::evaluate($conn, $scope, $args[1], $row);

        if (strpos($subject, '0000-00-00') === 0) {
            $format = str_replace(
                ['%Y', '%m', '%d', '%H', '%i', '%s'],
                ['0000', '00', '00', '00', '00', '00'],
                $format
            );
        }

        if (strpos($format, '%') === false) {
            return $format;
        }

        $format = \str_replace('%', '', $format);

        return (new \DateTimeImmutable($subject))->format($format);
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return array<string, mixed>
     */
    private static function maybeUnrollGroupedDataset(array $rows)
    {
        $first = reset($rows);
        if (\is_array($first)) {
            return $first;
        }
        return $rows;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateSub(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row) : string
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_SUB() function must be called with one arguments");
        }

        if (!$args[1] instanceof IntervalOperatorExpression) {
            throw new SQLFakeRuntimeException("MySQL DATE_SUB() arg 2 must be an interval");
        }

        $firstArg = Evaluator::evaluate($conn, $scope, $args[0], $row);

        return (new \DateTimeImmutable($firstArg))
            ->sub(self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateAdd(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row) : string
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_ADD() function must be called with one arguments");
        }

        if (!$args[1] instanceof IntervalOperatorExpression) {
            throw new SQLFakeRuntimeException("MySQL DATE_ADD() arg 2 must be an interval");
        }

        $firstArg = Evaluator::evaluate($conn, $scope, $args[0], $row);

        return (new \DateTimeImmutable($firstArg))
            ->add(self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlRound(FakePdo $conn, Scope $scope, FunctionExpression $expr, array $row) : float
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL ROUND() function must be called with one arguments");
        }

        $first = Evaluator::evaluate($conn, $scope, $args[0], $row);
        $second = Evaluator::evaluate($conn, $scope, $args[1], $row);

        return \round($first, $second);
    }

    private static function getPhpIntervalFromExpression(
        FakePdo $conn,
        Scope $scope, 
        IntervalOperatorExpression $expr,
        array $row
    ) : \DateInterval {
        $number = Evaluator::evaluate($conn, $scope, $expr->number, $row);

        switch ($expr->unit) {
            case 'DAY':
                return new \DateInterval('P' . $number . 'D');

            case 'MONTH':
                return new \DateInterval('P' . $number . 'M');

            case 'WEEK':
                return new \DateInterval('P' . $number . 'W');

            case 'YEAR':
                return new \DateInterval('P' . $number . 'Y');

            case 'MINUTE':
                return new \DateInterval('PT' . $number . 'M');

            case 'HOUR':
                return new \DateInterval('PT' . $number . 'H');

            case 'SECOND':
                return new \DateInterval('PT' . $number . 'S');

            default:
                throw new SQLFakeRuntimeException('MySQL INTERVAL unit ' . $expr->unit . ' not supported yet');
        }
    }
}

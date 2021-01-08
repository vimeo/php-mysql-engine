<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;

final class FunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    public static function evaluate(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        switch ($expr->functionName) {
            case 'COUNT':
                return self::sqlCount($expr, $row, $conn);
            case 'SUM':
                return (string) self::sqlSum($expr, $row, $conn);
            case 'MAX':
                return (string) self::sqlMax($expr, $row, $conn);
            case 'MIN':
                return (string) self::sqlMin($expr, $row, $conn);
            case 'MOD':
                return self::sqlMod($expr, $row, $conn);
            case 'AVG':
                return self::sqlAvg($expr, $row, $conn);
            case 'IF':
                return self::sqlIf($expr, $row, $conn);
            case 'IFNULL':
            case 'COALESCE':
                return self::sqlCoalesce($expr, $row, $conn);
            case 'NULLIF':
                return self::sqlNullif($expr, $row, $conn);
            case 'SUBSTRING':
            case 'SUBSTR':
                return self::sqlSubstring($expr, $row, $conn);
            case 'SUBSTRING_INDEX':
                return self::sqlSubstringIndex($expr, $row, $conn);
            case 'LENGTH':
                return self::sqlLength($expr, $row, $conn);
            case 'LOWER':
                return self::sqlLower($expr, $row, $conn);
            case 'UPPER':
                return self::sqlUpper($expr, $row, $conn);
            case 'CHAR_LENGTH':
            case 'CHARACTER_LENGTH':
                return self::sqlCharLength($expr, $row, $conn);
            case 'CONCAT_WS':
                return self::sqlConcatWS($expr, $row, $conn);
            case 'CONCAT':
                return self::sqlConcat($expr, $row, $conn);
            case 'FIELD':
                return self::sqlColumn($expr, $row, $conn);
            case 'BINARY':
                return self::sqlBinary($expr, $row, $conn);
            case 'FROM_UNIXTIME':
                return self::sqlFromUnixtime($expr, $row, $conn);
            case 'GREATEST':
                return self::sqlGreatest($expr, $row, $conn);
            case 'VALUES':
                return self::sqlValues($expr, $row, $conn);
            case 'NOW':
                return \date('Y-m-d H:i:s', time());
            case 'DATE':
                return self::sqlDate($expr, $row, $conn);
            case 'DATE_FORMAT':
                return self::sqlDateFormat($expr, $row, $conn);
            case 'ISNULL':
                return self::sqlIsNull($expr, $row, $conn);
            case 'DATE_SUB':
                return self::sqlDateSub($expr, $row, $conn);
            case 'DATE_ADD':
                return self::sqlDateAdd($expr, $row, $conn);
            case 'ROUND':
                return self::sqlRound($expr, $row, $conn);
        }

        throw new SQLFakeRuntimeException("Function " . $expr->functionName . " not implemented yet");
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return int
     */
    private static function sqlCount(FunctionExpression $expr, array $rows, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $inner = $expr->getExpr();

        if ($expr->distinct) {
            $buckets = [];
            foreach ($rows as $row) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();

                $val = Evaluator::evaluate($inner, $row, $conn);
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
            if (Evaluator::evaluate($inner, $row, $conn) !== null) {
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
    private static function sqlSum(FunctionExpression $expr, array $rows, \Vimeo\MysqlEngine\FakePdo $conn)
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
            $val = Evaluator::evaluate($expr, $row, $conn);
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
    private static function sqlMin(FunctionExpression $expr, array $rows, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($expr, $row, $conn);
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
    private static function sqlMax(FunctionExpression $expr, array $rows, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($expr, $row, $conn);
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
    private static function sqlMod(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL MOD() function must be called with two arguments");
        }

        $n = $args[0];
        $n_value = (int) Evaluator::evaluate($n, $row, $conn);
        $m = $args[1];
        $m_value = (int) Evaluator::evaluate($m, $row, $conn);

        return $n_value % $m_value;
    }

    /**
     * @param array<string, mixed> $rows
     *
     * @return mixed
     */
    private static function sqlAvg(FunctionExpression $expr, array $rows, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $value = Evaluator::evaluate($expr, $row, $conn);

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
    private static function sqlIf(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
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

        if ((bool) Evaluator::evaluate($condition, $row, $conn)) {
            $arg_to_evaluate = 1;
        }

        $expr = $args[$arg_to_evaluate];
        return Evaluator::evaluate($expr, $row, $conn);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlSubstring(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2 && \count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING() function must be called with two or three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);
        $position = $args[1];
        $pos = (int) Evaluator::evaluate($position, $row, $conn);
        $pos -= 1;
        $length = $args[2] ?? null;

        if ($length !== null) {
            $len = (int) Evaluator::evaluate($length, $row, $conn);
            return \mb_substr($string, $pos, $len);
        }

        return \mb_substr($string, $pos);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlSubstringIndex(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING_INDEX() function must be called with three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);
        $delimiter = $args[1];
        $delim = (string) Evaluator::evaluate($delimiter, $row, $conn);
        $pos = $args[2];

        if ($pos !== null) {
            $count = (int) Evaluator::evaluate($pos, $row, $conn);
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
    private static function sqlLower(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LOWER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);
        return \strtolower($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlUpper(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL UPPER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);
        return \strtoupper($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlLength(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);
        return \strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlBinary(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL BINARY() function must be called with one argument");
        }

        $subject = $args[0];
        return Evaluator::evaluate($subject, $row, $conn);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCharLength(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL CHAR_LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($subject, $row, $conn);

        return \mb_strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCoalesce(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        if (!\count($expr->args)) {
            throw new SQLFakeRuntimeException("MySQL COALESCE() function must be called with at least one argument");
        }

        foreach ($expr->args as $arg) {
            $eval_row = $row;

            if (!$arg->hasAggregate()) {
                $eval_row = self::maybeUnrollGroupedDataset($row);
            }

            $val = Evaluator::evaluate($arg, $eval_row, $conn);

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
    private static function sqlGreatest(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
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

            $val = Evaluator::evaluate($arg, $eval_row, $conn);
            $values[] = $val;
        }

        return \max($values);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlNullif(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL NULLIF() function must be called with two arguments");
        }

        $left = Evaluator::evaluate($args[0], $row, $conn);
        $right = Evaluator::evaluate($args[1], $row, $conn);

        return $left === $right ? null : $left;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlIsNull(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn) : int
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL ISNULL() function must be called with one arguments");
        }

        return Evaluator::evaluate($args[0], $row, $conn) === null ? 1 : 0;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlFromUnixtime(
        FunctionExpression $expr,
        array $row,
        \Vimeo\MysqlEngine\FakePdo $conn
    ) : string {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL FROM_UNIXTIME() SQLFake only implemented for 1 argument");
        }

        $column = Evaluator::evaluate($args[0], $row, $conn);
        $format = 'Y-m-d G:i:s';

        return \date($format, (int) $column);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcat(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT() function must be called with at least two arguments");
        }

        $final_concat = "";
        foreach ($args as $k => $arg) {
            $val = (string) Evaluator::evaluate($arg, $row, $conn);
            $final_concat .= $val;
        }

        return $final_concat;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcatWS(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function must be called with at least two arguments");
        }

        $separator = Evaluator::evaluate($args[0], $row, $conn);
        if ($separator === null) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function required non null separator");
        }

        $separator = (string) $separator;
        $final_concat = "";

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            $val = (string) Evaluator::evaluate($arg, $row, $conn);

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
    private static function sqlColumn(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args < 2) {
            throw new SQLFakeRuntimeException("MySQL FIELD() function must be called with at least two arguments");
        }

        $value = Evaluator::evaluate($args[0], $row, $conn);

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            if ($value == Evaluator::evaluate($arg, $row, $conn)) {
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
    private static function sqlValues(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
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

        return Evaluator::evaluate($arg, $row, $conn);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDate(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($args[0], $row, $conn);

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
    private static function sqlDateFormat(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn)
    {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_FORMAT() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($args[0], $row, $conn);
        $format = Evaluator::evaluate($args[1], $row, $conn);

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
    private static function sqlDateSub(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn) : string
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

        $firstArg = Evaluator::evaluate($args[0], $row, $conn);

        return (new \DateTimeImmutable($firstArg))
            ->sub(self::getPhpIntervalFromExpression($args[1], $row, $conn))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateAdd(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn) : string
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

        $firstArg = Evaluator::evaluate($args[0], $row, $conn);

        return (new \DateTimeImmutable($firstArg))
            ->add(self::getPhpIntervalFromExpression($args[1], $row, $conn))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlRound(FunctionExpression $expr, array $row, \Vimeo\MysqlEngine\FakePdo $conn) : float
    {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL ROUND() function must be called with one arguments");
        }

        $first = Evaluator::evaluate($args[0], $row, $conn);
        $second = Evaluator::evaluate($args[1], $row, $conn);

        return \round($first, $second);
    }

    private static function getPhpIntervalFromExpression(
        IntervalOperatorExpression $expr,
        array $row,
        \Vimeo\MysqlEngine\FakePdo $conn
    ) : \DateInterval {
        $number = Evaluator::evaluate($expr->number, $row, $conn);

        switch ($expr->unit) {
            case 'DAY':
                return new \DateInterval('P' . $number . 'D');

            case 'HOUR':
                return new \DateInterval('P' . $number . 'H');

            case 'MONTH':
                return new \DateInterval('P' . $number . 'M');

            case 'MINUTE':
                return new \DateInterval('PT' . $number . 'M');

            case 'SECOND':
                return new \DateInterval('P' . $number . 'S');

            case 'WEEK':
                return new \DateInterval('P' . $number . 'W');

            case 'YEAR':
                return new \DateInterval('P' . $number . 'Y');

            default:
                throw new SQLFakeRuntimeException('MySQL INTERVAL unit ' . $expr->unit . ' not supported yet');
        }
    }
}

<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\FakePdo;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Schema\Column;

final class FunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        switch ($expr->functionName) {
            case 'COUNT':
                return self::sqlCount($conn, $scope, $expr, $row, $columns);
            case 'SUM':
                return self::sqlSum($conn, $scope, $expr, $row, $columns);
            case 'MAX':
                return self::sqlMax($conn, $scope, $expr, $row, $columns);
            case 'MIN':
                return self::sqlMin($conn, $scope, $expr, $row, $columns);
            case 'MOD':
                return self::sqlMod($conn, $scope, $expr, $row, $columns);
            case 'AVG':
                return self::sqlAvg($conn, $scope, $expr, $row, $columns);
            case 'IF':
                return self::sqlIf($conn, $scope, $expr, $row, $columns);
            case 'IFNULL':
            case 'COALESCE':
                return self::sqlCoalesce($conn, $scope, $expr, $row, $columns);
            case 'NULLIF':
                return self::sqlNullif($conn, $scope, $expr, $row, $columns);
            case 'SUBSTRING':
            case 'SUBSTR':
                return self::sqlSubstring($conn, $scope, $expr, $row, $columns);
            case 'SUBSTRING_INDEX':
                return self::sqlSubstringIndex($conn, $scope, $expr, $row, $columns);
            case 'LENGTH':
                return self::sqlLength($conn, $scope, $expr, $row, $columns);
            case 'LOWER':
                return self::sqlLower($conn, $scope, $expr, $row, $columns);
            case 'UPPER':
                return self::sqlUpper($conn, $scope, $expr, $row, $columns);
            case 'CHAR_LENGTH':
            case 'CHARACTER_LENGTH':
                return self::sqlCharLength($conn, $scope, $expr, $row, $columns);
            case 'CONCAT_WS':
                return self::sqlConcatWS($conn, $scope, $expr, $row, $columns);
            case 'CONCAT':
                return self::sqlConcat($conn, $scope, $expr, $row, $columns);
            case 'FIELD':
                return self::sqlColumn($conn, $scope, $expr, $row, $columns);
            case 'BINARY':
                return self::sqlBinary($conn, $scope, $expr, $row, $columns);
            case 'FROM_UNIXTIME':
                return self::sqlFromUnixtime($conn, $scope, $expr, $row, $columns);
            case 'GREATEST':
                return self::sqlGreatest($conn, $scope, $expr, $row, $columns);
            case 'VALUES':
                return self::sqlValues($conn, $scope, $expr, $row, $columns);
            case 'NOW':
                return \date('Y-m-d H:i:s', time());
            case 'DATE':
                return self::sqlDate($conn, $scope, $expr, $row, $columns);
            case 'DATE_FORMAT':
                return self::sqlDateFormat($conn, $scope, $expr, $row, $columns);
            case 'ISNULL':
                return self::sqlIsNull($conn, $scope, $expr, $row, $columns);
            case 'DATE_SUB':
                return self::sqlDateSub($conn, $scope, $expr, $row, $columns);
            case 'DATE_ADD':
                return self::sqlDateAdd($conn, $scope, $expr, $row, $columns);
            case 'ROUND':
                return self::sqlRound($conn, $scope, $expr, $row, $columns);
            case 'DATEDIFF':
                return self::sqlDateDiff($conn, $scope, $expr, $row, $columns);
            case 'DAY':
                return self::sqlDay($conn, $scope, $expr, $row, $columns);
            case 'LAST_DAY':
                return self::sqlLastDay($conn, $scope, $expr, $row, $columns);
        }

        throw new SQLFakeRuntimeException("Function " . $expr->functionName . " not implemented yet");
    }

    /**
     * @param  array<string, Column> $columns
     * @return Column
     */
    public static function getColumnSchema(
        FunctionExpression $expr,
        Scope $scope,
        array $columns
    ) : Column {
        switch ($expr->functionName) {
            case 'COUNT':
                return new Column\IntColumn(true, 10);

            case 'SUM':
            case 'MAX':
            case 'MIN':
                $column = clone Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
                $column->isNullable = true;
                return $column;

            case 'MOD':
                return new Column\IntColumn(false, 10);
            case 'AVG':
                return new Column\FloatColumn(10, 2);
            case 'IF':
                $if = Evaluator::getColumnSchema($expr->args[1], $scope, $columns);
                $else = Evaluator::getColumnSchema($expr->args[2], $scope, $columns);

                if ($if->getPhpType() === 'string') {
                    return $if;
                }

                return $else;

            case 'IFNULL':
            case 'COALESCE':
                $if = Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
                $else = Evaluator::getColumnSchema($expr->args[1], $scope, $columns);

                if ($if instanceof Column\NullColumn) {
                    return $else;
                }

                $if = clone $if;
                $if->isNullable = false;

                if ($if->getPhpType() === 'string') {
                    return $if;
                }

                return $else;

            case 'NULLIF':
                break;

            case 'SUBSTRING':
            case 'SUBSTR':
                return new Column\Text();

            case 'SUBSTRING_INDEX':
                return new Column\Text();
            case 'LENGTH':
                return new Column\IntColumn(true, 10);
            case 'LOWER':
                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
            case 'UPPER':
                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
            case 'CHAR_LENGTH':
            case 'CHARACTER_LENGTH':
                return new Column\IntColumn(true, 10);
            case 'CONCAT_WS':
                return new Column\Text();
            case 'CONCAT':
                return new Column\Text();
            case 'FIELD':
                return new Column\IntColumn(true, 10);
            case 'BINARY':
                break;
            case 'FROM_UNIXTIME':
                return new Column\DateTime();
            case 'GREATEST':
                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
            case 'VALUES':
                break;
            case 'NOW':
                return new Column\DateTime();
            case 'DATE':
            case 'LAST_DAY':
                return new Column\Date();
            case 'DATE_FORMAT':
                return new Column\Varchar(255);
            case 'ISNULL':
                return new Column\TinyInt(true, 1);
            case 'DATE_SUB':
                return new Column\DateTime();
            case 'DATE_ADD':
                return new Column\DateTime();
            case 'ROUND':
                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);
            case 'DATEDIFF':
            case 'DAY':
                return new Column\IntColumn(false, 10);
        }

        // default type, a cop-out
        return new Column\Varchar(255);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return int
     */
    private static function sqlCount(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $rows,
        array $columns
    ) {
        $inner = $expr->getExpr();

        if ($expr->distinct) {
            $buckets = [];
            foreach ($rows as $row) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();

                $val = Evaluator::evaluate($conn, $scope, $inner, $row, $columns);
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
            if (Evaluator::evaluate($conn, $scope, $inner, $row, $columns) !== null) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return numeric
     */
    private static function sqlSum(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $rows,
        array $columns
    ) {
        $expr = $expr->getExpr();

        $sum = 0;

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $val = Evaluator::evaluate($conn, $scope, $expr, $row, $columns);
            $num = \is_int($val) ? $val : (double) $val;
            $sum += $num;
        }

        return self::castAggregate($sum, $expr, $columns);
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function castAggregate($value, Expression $expr, array $columns)
    {
        $column = null;

        if ($expr->name && isset($columns[$expr->name])) {
            $column = $columns[$expr->name];
        }

        if ($column) {
            switch ($column->getPhpType()) {
                case 'int':
                    return (int) $value;

                case 'float':
                    return (float) $value;

                case 'string':
                    if ($column instanceof \Vimeo\MysqlEngine\Schema\Column\Decimal) {
                        /** @var numeric-string */
                        return \number_format($value, $column->getDecimalScale(), '.', '');
                    }
            }
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMin(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $rows,
        array $columns
    ) {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($conn, $scope, $expr, $row, $columns);
        }

        if (0 === \count($values)) {
            return null;
        }

        return self::castAggregate(\min($values), $expr, $columns);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMax(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $rows,
        array $columns
    ) {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $values[] = Evaluator::evaluate($conn, $scope, $expr, $row, $columns);
        }

        if (0 === \count($values)) {
            return null;
        }

        return self::castAggregate(\max($values), $expr, $columns);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMod(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL MOD() function must be called with two arguments");
        }

        $n = $args[0];
        $n_value = (int) Evaluator::evaluate($conn, $scope, $n, $row, $columns);
        $m = $args[1];
        $m_value = (int) Evaluator::evaluate($conn, $scope, $m, $row, $columns);

        return $n_value % $m_value;
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlAvg(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $rows,
        array $columns
    ) {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $value = Evaluator::evaluate($conn, $scope, $expr, $row, $columns);

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
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlIf(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL IF() function must be called with three arguments");
        }

        $condition = $args[0];
        $arg_to_evaluate = 2;

        if ((bool) Evaluator::evaluate($conn, $scope, $condition, $row, $columns)) {
            $arg_to_evaluate = 1;
        }

        $expr = $args[$arg_to_evaluate];
        return Evaluator::evaluate($conn, $scope, $expr, $row, $columns);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlSubstring(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2 && \count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING() function must be called with two or three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
        $position = $args[1];
        $pos = (int) Evaluator::evaluate($conn, $scope, $position, $row, $columns);
        $pos -= 1;
        $length = $args[2] ?? null;

        if ($length !== null) {
            $len = (int) Evaluator::evaluate($conn, $scope, $length, $row, $columns);
            return \mb_substr($string, $pos, $len);
        }

        return \mb_substr($string, $pos);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlSubstringIndex(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new SQLFakeRuntimeException("MySQL SUBSTRING_INDEX() function must be called with three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
        $delimiter = $args[1];
        $delim = (string) Evaluator::evaluate($conn, $scope, $delimiter, $row, $columns);
        $pos = $args[2];

        if ($pos !== null) {
            $count = (int) Evaluator::evaluate($conn, $scope, $pos, $row, $columns);
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
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlLower(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LOWER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
        return \strtolower($string);
    }

    /**
     * @param array<string, mixed> $rows
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlUpper(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL UPPER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
        return \strtoupper($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlLength(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
        return \strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlBinary(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL BINARY() function must be called with one argument");
        }

        $subject = $args[0];
        return Evaluator::evaluate($conn, $scope, $subject, $row, $columns);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCharLength(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL CHAR_LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $columns);

        return \mb_strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCoalesce(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        if (!\count($expr->args)) {
            throw new SQLFakeRuntimeException("MySQL COALESCE() function must be called with at least one argument");
        }

        foreach ($expr->args as $arg) {
            $eval_row = $row;

            if (!$arg->hasAggregate()) {
                $eval_row = self::maybeUnrollGroupedDataset($row);
            }

            $val = Evaluator::evaluate($conn, $scope, $arg, $eval_row, $columns);

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
    private static function sqlGreatest(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
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

            $val = Evaluator::evaluate($conn, $scope, $arg, $eval_row, $columns);
            $values[] = $val;
        }

        return \max($values);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlNullif(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL NULLIF() function must be called with two arguments");
        }

        $left = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        $right = Evaluator::evaluate($conn, $scope, $args[1], $row, $columns);

        return $left === $right ? null : $left;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlIsNull(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : int {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL ISNULL() function must be called with one arguments");
        }

        return Evaluator::evaluate($conn, $scope, $args[0], $row, $columns) === null ? 1 : 0;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlFromUnixtime(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : string {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL FROM_UNIXTIME() SQLFake only implemented for 1 argument");
        }

        $column = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        $format = 'Y-m-d G:i:s';

        return \date($format, (int) $column);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcat(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT() function must be called with at least two arguments");
        }

        $final_concat = "";
        foreach ($args as $k => $arg) {
            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row, $columns);
            $final_concat .= $val;
        }

        return $final_concat;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcatWS(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function must be called with at least two arguments");
        }

        $separator = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        if ($separator === null) {
            throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function required non null separator");
        }

        $separator = (string) $separator;
        $final_concat = "";

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row, $columns);

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
    private static function sqlColumn(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args < 2) {
            throw new SQLFakeRuntimeException("MySQL FIELD() function must be called with at least two arguments");
        }

        $value = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            if ($value == Evaluator::evaluate($conn, $scope, $arg, $row, $columns)) {
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
    private static function sqlValues(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
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

        return Evaluator::evaluate($conn, $scope, $arg, $row, $columns);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDate(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        if (!$subject || \strpos($subject, '0000-00-00') === 0) {
            return '0000-00-00';
        }

        return (new \DateTimeImmutable($subject))->format('Y-m-d');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlLastDay(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : ?string {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        if (!$subject || \strpos($subject, '0000-00-00') === 0) {
            return null;
        }

        return (new \DateTimeImmutable($subject))->format('Y-m-t');
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDateFormat(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) {
        $row = self::maybeUnrollGroupedDataset($row);
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_FORMAT() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        $format = Evaluator::evaluate($conn, $scope, $args[1], $row, $columns);

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
     * @param array<string, Column> $columns
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
    private static function sqlDateSub(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : string {
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

        $firstArg = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        return (new \DateTimeImmutable($firstArg))
            ->sub(self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row, $columns))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateAdd(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : string {
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

        $firstArg = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        $interval = self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row, $columns);

        $first_date = new \DateTimeImmutable($firstArg);

        $candidate = $first_date->add($interval);

        // mimic behaviour of MySQL for leap years and other rollover dates
        if (($interval->m || $interval->y)
            && (int) $first_date->format('d') >= 28
            && ($candidate->format('d') !== $first_date->format('d'))
        ) {
            // remove a week
            $candidate = $candidate->sub(new \DateInterval('P7D'));
            // then get the last day
            return $candidate->format('Y-m-t H:i:s');
        }

        return $candidate->format('Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateDiff(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : int {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL DATE_ADD() function must be called with one arguments");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        $second_arg = Evaluator::evaluate($conn, $scope, $args[1], $row, $columns);

        return (new \DateTimeImmutable($first_arg))->diff(new \DateTimeImmutable($second_arg))->days;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDay(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : int {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new SQLFakeRuntimeException("MySQL DATE_ADD() function must be called with one arguments");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);

        return (int)(new \DateTimeImmutable($first_arg))->format('d');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlRound(
        FakePdo $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        array $columns
    ) : float {
        if (!$expr->hasAggregate()) {
            $row = self::maybeUnrollGroupedDataset($row);
        }

        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new SQLFakeRuntimeException("MySQL ROUND() function must be called with one arguments");
        }

        $first = Evaluator::evaluate($conn, $scope, $args[0], $row, $columns);
        $second = Evaluator::evaluate($conn, $scope, $args[1], $row, $columns);

        return \round($first, $second);
    }

    private static function getPhpIntervalFromExpression(
        FakePdo $conn,
        Scope $scope,
        IntervalOperatorExpression $expr,
        array $row,
        array $columns
    ) : \DateInterval {
        $number = Evaluator::evaluate($conn, $scope, $expr->number, $row, $columns);

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

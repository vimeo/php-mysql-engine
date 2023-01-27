<?php
namespace Vimeo\MysqlEngine\Processor\Expression;

use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Schema\Column;
use Vimeo\MysqlEngine\TokenType;

final class FunctionEvaluator
{
    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    public static function evaluate(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        switch ($expr->functionName) {
            case 'COUNT':
                return self::sqlCount($conn, $scope, $expr, $result);
            case 'SUM':
                return self::sqlSum($conn, $scope, $expr, $result);
            case 'MAX':
                return self::sqlMax($conn, $scope, $expr, $result);
            case 'MIN':
                return self::sqlMin($conn, $scope, $expr, $result);
            case 'AVG':
                return self::sqlAvg($conn, $scope, $expr, $result);
            case 'MOD':
                return self::sqlMod($conn, $scope, $expr, $row, $result);
            case 'IF':
                return self::sqlIf($conn, $scope, $expr, $row, $result);
            case 'IFNULL':
            case 'COALESCE':
                return self::sqlCoalesce($conn, $scope, $expr, $row, $result);
            case 'NULLIF':
                return self::sqlNullif($conn, $scope, $expr, $row, $result);
            case 'SUBSTRING':
            case 'SUBSTR':
                return self::sqlSubstring($conn, $scope, $expr, $row, $result);
            case 'SUBSTRING_INDEX':
                return self::sqlSubstringIndex($conn, $scope, $expr, $row, $result);
            case 'LENGTH':
                return self::sqlLength($conn, $scope, $expr, $row, $result);
            case 'LOWER':
                return self::sqlLower($conn, $scope, $expr, $row, $result);
            case 'UPPER':
                return self::sqlUpper($conn, $scope, $expr, $row, $result);
            case 'CHAR_LENGTH':
            case 'CHARACTER_LENGTH':
                return self::sqlCharLength($conn, $scope, $expr, $row, $result);
            case 'CONCAT_WS':
                return self::sqlConcatWS($conn, $scope, $expr, $row, $result);
            case 'CONCAT':
                return self::sqlConcat($conn, $scope, $expr, $row, $result);
            case 'FIELD':
                return self::sqlColumn($conn, $scope, $expr, $row, $result);
            case 'BINARY':
                return self::sqlBinary($conn, $scope, $expr, $row, $result);
            case 'FROM_UNIXTIME':
                return self::sqlFromUnixtime($conn, $scope, $expr, $row, $result);
            case 'UNIX_TIMESTAMP':
                return self::sqlUnixTimestamp($conn, $scope, $expr, $row, $result);
            case 'GREATEST':
                return self::sqlGreatest($conn, $scope, $expr, $row, $result);
            case 'VALUES':
                return self::sqlValues($conn, $scope, $expr, $row, $result);
            case 'NOW':
                return \date('Y-m-d H:i:s', time());
            case 'DATE':
                return self::sqlDate($conn, $scope, $expr, $row, $result);
            case 'DATE_FORMAT':
                return self::sqlDateFormat($conn, $scope, $expr, $row, $result);
            case 'ISNULL':
                return self::sqlIsNull($conn, $scope, $expr, $row, $result);
            case 'DATE_SUB':
                return self::sqlDateSub($conn, $scope, $expr, $row, $result);
            case 'DATE_ADD':
                return self::sqlDateAdd($conn, $scope, $expr, $row, $result);
            case 'ROUND':
                return self::sqlRound($conn, $scope, $expr, $row, $result);
            case 'CEIL':
            case 'CEILING':
                return self::sqlCeiling($conn, $scope, $expr, $row, $result);
            case 'FLOOR':
                return self::sqlFloor($conn, $scope, $expr, $row, $result);
            case 'DATEDIFF':
                return self::sqlDateDiff($conn, $scope, $expr, $row, $result);
            case 'DAY':
                return self::sqlDay($conn, $scope, $expr, $row, $result);
            case 'LAST_DAY':
                return self::sqlLastDay($conn, $scope, $expr, $row, $result);
            case 'CURDATE':
            case 'CURRENT_DATE':
                return self::sqlCurDate($expr);
            case 'WEEKDAY':
                return self::sqlWeekDay($conn, $scope, $expr, $row, $result);
            case 'INET_ATON':
                return self::sqlInetAton($conn, $scope, $expr, $row, $result);
            case 'INET_NTOA':
                return self::sqlInetNtoa($conn, $scope, $expr, $row, $result);
        }

        throw new ProcessorException("Function " . $expr->functionName . " not implemented yet");
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

                if ($column instanceof Column\IntegerColumn) {
                    $column = new Column\BigInt(false, 10);
                }

                $column->setNullable(true);
                return $column;

            case 'MOD':
                return new Column\IntColumn(false, 10);

            case 'AVG':
                return new Column\FloatColumn(10, 2);

            case 'CEIL':
            case 'CEILING':
            case 'FLOOR':
                // from MySQL docs: https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_ceil
                // For exact-value numeric arguments, the return value has an exact-value numeric type. For string or
                // floating-point arguments, the return value has a floating-point type.  But...
                //
                // mysql> CREATE TEMPORARY TABLE `temp` SELECT FLOOR(1.2);
                // Query OK, 1 row affected (0.00 sec)
                // Records: 1  Duplicates: 0  Warnings: 0
                //
                // mysql> describe temp;
                // +------------+--------+------+-----+---------+-------+
                // | Field      | Type   | Null | Key | Default | Extra |
                // +------------+--------+------+-----+---------+-------+
                // | FLOOR(1.2) | bigint | NO   |     | 0       | NULL  |
                // +------------+--------+------+-----+---------+-------+
                // 1 row in set (0.00 sec)
                if ($expr->args[0]->getType() == TokenType::STRING_CONSTANT) {
                    return new Column\DoubleColumn(10, 2);
                }

                return new Column\BigInt(false, 10);

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
                $if->setNullable(false);

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

            case 'CURDATE':
            case 'CURRENT_DATE':
                return new Column\Date();

            case 'DATE':
            case 'LAST_DAY':
                $arg = Evaluator::getColumnSchema($expr->args[0], $scope, $columns);

                $date = new Column\Date();

                if ($arg->isNullable()) {
                    $date->setNullable(true);
                }

                return $date;

            case 'DATE_FORMAT':
                return new Column\Varchar(255);

            case 'ISNULL':
                return new Column\TinyInt(true, 1);

            case 'DATE_SUB':
            case 'DATE_ADD':
                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);

            case 'ROUND':
                $precision = 0;

                if (isset($expr->args[1])) {
                    /** @var ConstantExpression $arg */
                    $arg = $expr->args[1];

                    $precision = (int)$arg->value;
                }

                if ($precision === 0) {
                    return new Column\IntColumn(false, 10);
                }

                return Evaluator::getColumnSchema($expr->args[0], $scope, $columns);

            case 'INET_ATON':
                return new Column\IntColumn(true, 15);

            case 'DATEDIFF':
            case 'DAY':
            case 'WEEKDAY':
                return new Column\IntColumn(false, 10);
        }

        // default type, a cop-out
        return new Column\Varchar(255);
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return int
     */
    private static function sqlCount(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        QueryResult $result
    ) {
        $inner = $expr->getExpr();

        if ($expr->distinct) {
            $buckets = [];
            foreach ($result->rows as $row) {
                \is_array($row) ? $row : (function () {
                    throw new \TypeError('Failed assertion');
                })();

                $val = Evaluator::evaluate($conn, $scope, $inner, $row, $result);
                if (\is_int($val) || \is_string($val)) {
                    $buckets[$val] = 1;
                }
            }

            return \count($buckets);
        }

        $count = 0;
        foreach ($result->rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            if (Evaluator::evaluate($conn, $scope, $inner, $row, $result) !== null) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return ?numeric
     */
    private static function sqlSum(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        QueryResult $result
    ) {
        $expr = $expr->getExpr();

        $sum = 0;

        if (!$result->rows) {
            return null;
        }

        foreach ($result->rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();
            $val = Evaluator::evaluate($conn, $scope, $expr, $row, $result);
            $num = \is_int($val) ? $val : (double) $val;
            $sum += $num;
        }

        return self::castAggregate($sum, $expr, $result);
    }

    /**
     * @param array<string, Column> $columns
     * @param scalar $value
     *
     * @return mixed
     */
    private static function castAggregate($value, Expression $expr, QueryResult $result)
    {
        $column = null;

        if ($expr->name && isset($result->columns[$expr->name])) {
            $column = $result->columns[$expr->name];
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
                        return \number_format((float) $value, $column->getDecimalScale(), '.', '');
                    }
            }
        }

        return $value;
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMin(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        QueryResult $result
    ) {
        $expr = $expr->getExpr();
        $values = [];

        if (!$result->rows) {
            return null;
        }

        foreach ($result->rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $value = Evaluator::evaluate($conn, $scope, $expr, $row, $result);

            if (!\is_scalar($value)) {
                throw new \TypeError('Bad min value');
            }

            $values[] = $value;
        }

        return self::castAggregate(\min($values), $expr, $result);
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMax(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        QueryResult $result
    ) {
        $expr = $expr->getExpr();
        $values = [];

        if (!$result->rows) {
            return null;
        }

        foreach ($result->rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            $value = Evaluator::evaluate($conn, $scope, $expr, $row, $result);

            if (!\is_scalar($value)) {
                throw new \TypeError('Bad max value');
            }

            $values[] = $value;
        }

        return self::castAggregate(\max($values), $expr, $result);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlMod(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL MOD() function must be called with two arguments");
        }

        $n = $args[0];
        $n_value = (int) Evaluator::evaluate($conn, $scope, $n, $row, $result);
        $m = $args[1];
        $m_value = (int) Evaluator::evaluate($conn, $scope, $m, $row, $result);

        return $n_value % $m_value;
    }

    /**
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlAvg(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        QueryResult $result
    ) {
        $expr = $expr->getExpr();
        $values = [];

        foreach ($result->rows as $row) {
            \is_array($row) ? $row : (function () {
                throw new \TypeError('Failed assertion');
            })();

            /**
             * @var float|int
             */
            $value = Evaluator::evaluate($conn, $scope, $expr, $row, $result);

            $values[] = $value;
        }

        if (\count($values) === 0) {
            return null;
        }

        return \array_sum($values) / \count($values);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlIf(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new ProcessorException("MySQL IF() function must be called with three arguments");
        }

        $condition = $args[0];
        $arg_to_evaluate = 2;

        if ((bool) Evaluator::evaluate($conn, $scope, $condition, $row, $result)) {
            $arg_to_evaluate = 1;
        }

        $expr = $args[$arg_to_evaluate];
        return Evaluator::evaluate($conn, $scope, $expr, $row, $result);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlSubstring(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 2 && \count($args) !== 3) {
            throw new ProcessorException("MySQL SUBSTRING() function must be called with two or three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);
        $position = $args[1];
        $pos = (int) Evaluator::evaluate($conn, $scope, $position, $row, $result);
        $pos -= 1;
        $length = $args[2] ?? null;

        if ($length !== null) {
            $len = (int) Evaluator::evaluate($conn, $scope, $length, $row, $result);
            return \mb_substr($string, $pos, $len);
        }

        return \mb_substr($string, $pos);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlSubstringIndex(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 3) {
            throw new ProcessorException("MySQL SUBSTRING_INDEX() function must be called with three arguments");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);
        $delimiter = $args[1];
        $delim = (string) Evaluator::evaluate($conn, $scope, $delimiter, $row, $result);
        $pos = $args[2];

        if ($pos !== null && $delim !== '') {
            $count = (int) Evaluator::evaluate($conn, $scope, $pos, $row, $result);
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
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlLower(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL LOWER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);
        return \strtolower($string);
    }

    /**
     * @param array<string, mixed> $row
     * @param array<string, Column> $columns
     *
     * @return mixed
     */
    private static function sqlUpper(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL UPPER() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);
        return \strtoupper($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlLength(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);
        return \strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlBinary(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL BINARY() function must be called with one argument");
        }

        $subject = $args[0];
        return Evaluator::evaluate($conn, $scope, $subject, $row, $result);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCharLength(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL CHAR_LENGTH() function must be called with one argument");
        }

        $subject = $args[0];
        $string = (string) Evaluator::evaluate($conn, $scope, $subject, $row, $result);

        return \mb_strlen($string);
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlCoalesce(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        if (!\count($expr->args)) {
            throw new ProcessorException("MySQL COALESCE() function must be called with at least one argument");
        }

        foreach ($expr->args as $arg) {
            $val = Evaluator::evaluate($conn, $scope, $arg, $row, $result);

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
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new ProcessorException("MySQL GREATEST() function must be called with at two arguments");
        }

        $values = [];
        foreach ($expr->args as $arg) {
            $val = Evaluator::evaluate($conn, $scope, $arg, $row, $result);
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
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL NULLIF() function must be called with two arguments");
        }

        $left = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);
        $right = Evaluator::evaluate($conn, $scope, $args[1], $row, $result);

        return $left === $right ? null : $left;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlIsNull(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : int {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL ISNULL() function must be called with one arguments");
        }

        return Evaluator::evaluate($conn, $scope, $args[0], $row, $result) === null ? 1 : 0;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlFromUnixtime(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : string {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL FROM_UNIXTIME() SQLFake only implemented for 1 argument");
        }

        $column = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        return \date('Y-m-d H:i:s', (int) $column);
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlUnixTimestamp(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?int {
        $args = $expr->args;

        switch (\count($args)) {
            case 0:
                return time();
            case 1:
                $column = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);
                if (!\is_string($column)) {
                    return null;
                }
                return \strtotime($column) ?: null;
            default:
                throw new ProcessorException("MySQL UNIX_TIMESTAPM() SQLFake only implemented for 0 or 1 argument");
        }
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return string
     */
    private static function sqlConcat(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new ProcessorException("MySQL CONCAT() function must be called with at least two arguments");
        }

        $final_concat = "";
        foreach ($args as $arg) {
            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row, $result);
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
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) < 2) {
            throw new ProcessorException("MySQL CONCAT_WS() function must be called with at least two arguments");
        }

        $separator = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);
        if ($separator === null) {
            throw new ProcessorException("MySQL CONCAT_WS() function required non null separator");
        }

        $separator = (string) $separator;
        $final_concat = "";

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            $val = (string) Evaluator::evaluate($conn, $scope, $arg, $row, $result);

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
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args < 2) {
            throw new ProcessorException("MySQL FIELD() function must be called with at least two arguments");
        }

        $value = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!\is_scalar($value)) {
            throw new ProcessorException("MySQL COLUMN first arg must be a scalar");
        }

        foreach ($args as $k => $arg) {
            if ($k < 1) {
                continue;
            }

            if ($value == Evaluator::evaluate($conn, $scope, $arg, $row, $result)) {
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
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;
        $num_args = \count($args);

        if ($num_args !== 1) {
            throw new ProcessorException("MySQL VALUES() function must be called with one argument");
        }

        $arg = $args[0];
        if (!$arg instanceof ColumnExpression) {
            throw new ProcessorException("MySQL VALUES() function should be called with a column name");
        }

        if (\substr($arg->columnExpression, 0, 16) !== 'sql_fake_values.') {
            $arg->columnExpression = 'sql_fake_values.' . $arg->columnExpression;
        }

        return Evaluator::evaluate($conn, $scope, $arg, $row, $result);
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDate(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?string {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!$subject) {
            return null;
        }

        if (\strpos($subject, '0000-00-00') === 0) {
            return '0000-00-00';
        }

        return (new \DateTimeImmutable($subject))->format('Y-m-d');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlLastDay(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?string {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL DATE() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!$subject || \strpos($subject, '0000-00-00') === 0) {
            return null;
        }

        return (new \DateTimeImmutable($subject))->format('Y-m-t');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlCurDate(FunctionExpression $expr): string
    {
        $args = $expr->args;

        if (\count($args) !== 0) {
            throw new ProcessorException("MySQL CURDATE() function takes no arguments.");
        }

        return (new \DateTimeImmutable())->format('Y-m-d');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlWeekDay(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?int {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL WEEKDAY() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!is_string($subject)) {
            throw new \TypeError('Failed assertion');
        }

        if (!$subject || \strpos($subject, '0000-00-00') === 0) {
            return null;
        }

        return (int)(new \DateTimeImmutable($subject))->format('N');
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return mixed
     */
    private static function sqlDateFormat(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL DATE_FORMAT() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);
        $format = Evaluator::evaluate($conn, $scope, $args[1], $row, $result);

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
     * @param array<string, mixed> $row
     */
    private static function sqlDateSub(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?string {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL DATE_SUB() function must be called with one arguments");
        }

        if (!$args[1] instanceof IntervalOperatorExpression) {
            throw new ProcessorException("MySQL DATE_SUB() arg 2 must be an interval");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if ($first_arg === null) {
            return null;
        }

        $first_arg = trim($first_arg);

        $interval = self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row, $result);

        $first_date = new \DateTimeImmutable($first_arg);

        $candidate = $first_date->sub($interval);

        // mimic behaviour of MySQL for leap years and other rollover dates
        if (($interval->m || $interval->y)
            && (int) $first_date->format('d') >=28
            && ($candidate->format('d') !== $first_date->format('d'))
        ) {
            // remove a week
            $candidate = $candidate->sub(new \DateInterval('P7D'));
            // then get the last day
            return $candidate->format(\strlen($first_arg) === 10 ? 'Y-m-t' : 'Y-m-t H:i:s');
        }

        return $candidate->format(\strlen($first_arg) === 10 ? 'Y-m-d' : 'Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateAdd(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?string {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL DATE_ADD() function must be called with one arguments");
        }

        if (!$args[1] instanceof IntervalOperatorExpression) {
            throw new ProcessorException("MySQL DATE_ADD() arg 2 must be an interval");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if ($first_arg === null) {
            return null;
        }

        $first_arg = trim($first_arg);

        $interval = self::getPhpIntervalFromExpression($conn, $scope, $args[1], $row, $result);

        $first_date = new \DateTimeImmutable($first_arg);

        $candidate = $first_date->add($interval);

        // mimic behaviour of MySQL for leap years and other rollover dates
        if (($interval->m || $interval->y)
            && (int) $first_date->format('d') >= 28
            && ($candidate->format('d') !== $first_date->format('d'))
        ) {
            // remove a week
            $candidate = $candidate->sub(new \DateInterval('P7D'));
            // then get the last day
            return $candidate->format(\strlen($first_arg) === 10 ? 'Y-m-t' : 'Y-m-t H:i:s');
        }

        return $candidate->format(\strlen($first_arg) === 10 ? 'Y-m-d' : 'Y-m-d H:i:s');
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDateDiff(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : int {
        $args = $expr->args;

        if (\count($args) !== 2) {
            throw new ProcessorException("MySQL DATE_ADD() function must be called with one arguments");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);
        $second_arg = Evaluator::evaluate($conn, $scope, $args[1], $row, $result);

        return (new \DateTimeImmutable($first_arg))->diff(new \DateTimeImmutable($second_arg))->days;
    }

    /**
     * @param array<string, mixed> $row
     */
    private static function sqlDay(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : int {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL DATE_ADD() function must be called with one arguments");
        }

        $first_arg = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        return (int)(new \DateTimeImmutable($first_arg))->format('d');
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return float|int
     */
    private static function sqlRound(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1 && \count($args) !== 2) {
            throw new ProcessorException("MySQL ROUND() function must be called with one or two arguments");
        }

        $number = (float)Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!isset($args[1])) {
            return \round($number);
        }

        $precision = (int)Evaluator::evaluate($conn, $scope, $args[1], $row, $result);

        return \round($number, $precision);
    }

    /**
     * @param array<string, mixed> $row
     * @return float|null
     */
    private static function sqlInetAton(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?float {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL INET_ATON() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!is_string($subject)) {
            // INET_ATON() returns NULL if it does not understand its argument.
            return null;
        }

        $value = ip2long($subject);

        if (!$value) {
            return null;
        }

        // https://www.php.net/manual/en/function.ip2long.php - this comes as a signed int
        //use %u to convert this to an unsigned long, then cast it as a float
        return floatval(sprintf('%u', $value));
    }

    /**
     * @param array<string, mixed> $row
     * @return string
     */
    private static function sqlInetNtoa(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) : ?string {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL INET_NTOA() function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!is_numeric($subject)) {
            // INET_NTOA() returns NULL if it does not understand its argument
            return null;
        }

        return long2ip((int)$subject);
    }

    /**
     * @param array<string, mixed> $row
     * @return float|0
     */
    private static function sqlCeiling(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL CEILING function must be called with one argument (got " . count($args) . ")");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!is_numeric($subject)) {
            // CEILING() returns 0 if it does not understand its argument.
            return 0;
        }

        $value = ceil(floatval($subject));

        if (!$value) {
            return 0;
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $row
     * @return float|0
     */
    private static function sqlFloor(
        FakePdoInterface $conn,
        Scope $scope,
        FunctionExpression $expr,
        array $row,
        QueryResult $result
    ) {
        $args = $expr->args;

        if (\count($args) !== 1) {
            throw new ProcessorException("MySQL FLOOR function must be called with one argument");
        }

        $subject = Evaluator::evaluate($conn, $scope, $args[0], $row, $result);

        if (!is_numeric($subject)) {
            // FLOOR() returns 0 if it does not understand its argument.
            return 0;
        }

        $value = floor(floatval($subject));

        if (!$value) {
            return 0;
        }

        return $value;
    }

    private static function getPhpIntervalFromExpression(
        FakePdoInterface $conn,
        Scope $scope,
        IntervalOperatorExpression $expr,
        array $row,
        QueryResult $result
    ) : \DateInterval {
        $number = Evaluator::evaluate($conn, $scope, $expr->number, $row, $result);

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
                throw new ProcessorException('MySQL INTERVAL unit ' . $expr->unit . ' not supported yet');
        }
    }
}

<?php
namespace Vimeo\MysqlEngine;

final class DataIntegrity
{
    /**
     * @return mixed
     */
    protected static function getDefaultValueForColumn(
        FakePdoInterface $conn,
        string $php_type,
        bool $nullable,
        ?string $default,
        ?string $column_name = null,
        ?string $database_name = null,
        ?string $table_name = null,
        bool $auto_increment = false
    ) {
        if ($default !== null) {
            if ($default === 'CURRENT_TIMESTAMP') {
                $default = \date('Y-m-d H:i:s', time());
            }

            switch ($php_type) {
                case 'int':
                    return (int) $default;
                case 'float':
                    return (float) $default;
                default:
                    return $default;
            }
        }

        if ($nullable) {
            return null;
        }

        if ($auto_increment && $column_name && $database_name && $table_name) {
            return $conn->getServer()->getNextAutoIncrementValue($database_name, $table_name, $column_name);
        }

        switch ($php_type) {
            case 'int':
                return 0;

            case 'float':
                return 0.0;

            default:
                return '';
        }
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    public static function ensureColumnsPresent(
        FakePdoInterface $conn,
        array $row,
        Schema\TableDefinition $table_definition
    ) {
        foreach ($table_definition->columns as $column_name => $column) {
            $php_type = $column->getPhpType();
            $column_nullable = $column->isNullable;

            $column_default = $column instanceof Schema\Column\Defaultable ? $column->getDefault() : null;

            if (!array_key_exists($column_name, $row)) {
                $row[$column_name] = self::getDefaultValueForColumn(
                    $conn,
                    $php_type,
                    $column_nullable,
                    $column_default,
                    $column_name,
                    $table_definition->databaseName,
                    $table_definition->name,
                    $column instanceof Schema\Column\IntegerColumn && $column->isAutoIncrement()
                );
            } else {
                if ($row[$column_name] === null) {
                    if ($column_nullable) {
                        continue;
                    } else {
                        $row[$column_name] = self::coerceValueToColumn($conn, $column, null);
                    }
                } else {
                    switch ($php_type) {
                        case 'int':
                            if (\is_bool($row[$column_name])) {
                                $row[$column_name] = (int) $row[$column_name];
                            } else {
                                if (!\is_int($row[$column_name])) {
                                    $row[$column_name] = (int) $row[$column_name];
                                }
                            }
                            break;
                        case 'float':
                            if (!\is_float($row[$column_name])) {
                                $row[$column_name] = (double) $row[$column_name];
                            }
                            break;
                        default:
                            if (!\is_string($row[$column_name])) {
                                $row[$column_name] = (string) $row[$column_name];
                            }
                            break;
                    }
                }
            }
        }
        return $row;
    }

    /**
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    public static function coerceToSchema(
        FakePdoInterface $conn,
        array $row,
        Schema\TableDefinition $table_definition
    ) {
        $row = self::ensureColumnsPresent($conn, $row, $table_definition);

        foreach ($row as $column_name => $value) {
            if (!isset($table_definition->columns[$column_name])) {
                throw new \Exception(
                    "Column '$column_name' not found on '{$table_definition->name}', expecting "
                        . '\'' . implode('\', \'', array_keys($table_definition->columns)) . '\''
                );
            }

            $column = $table_definition->columns[$column_name];

            $row[$column_name] = self::coerceValueToColumn($conn, $column, $value);
        }

        return $row;
    }

    /**
     * @return false|float|int|null|string
     */
    public static function coerceValueToColumn(
        FakePdoInterface $conn,
        Schema\Column $column,
        $value
    ) {
        $php_type = $column->getPhpType();

        if ($column->isNullable && $value === null) {
            return null;
        }

        if ($column instanceof Schema\Column\NumberColumn) {
            if ($value === '') {
                $value = 0;
            } elseif (\is_bool($value)) {
                $value = (int) $value;
            }

            if (!\is_numeric($value)) {
                throw new Processor\InvalidValueException('Number column expects a numeric value, but saw ' . $value);
            }

            if ((float) $value > $column->getMaxValue()) {
                if ($conn->useStrictMode()) {
                    throw new Processor\InvalidValueException('Value ' . $value . ' out of acceptable range');
                }

                $value = $column->getMaxValue();
            } elseif ((float) $value < $column->getMinValue()) {
                if ($conn->useStrictMode()) {
                    throw new Processor\InvalidValueException('Value ' . $value . ' out of acceptable range');
                }

                $value = $column->getMinValue();
            }
        }

        if ($column instanceof Schema\Column\CharacterColumn) {
            if (!is_string($value)) {
                $value = (string) $value;
            }

            if (\strlen($value) > $column->getMaxStringLength()) {
                if ($conn->useStrictMode()) {
                    throw new Processor\InvalidValueException('String length for ' . $value . ' larger than expected');
                }

                $value = \substr($value, 0, $column->getMaxStringLength());
            }
        }

        switch ($php_type) {
            case 'int':
                return (int) $value;

            case 'string':
                $value = (string) $value;

                if ($column instanceof Schema\Column\DateTime || $column instanceof Schema\Column\Timestamp) {
                    if (\strlen($value) === 10) {
                        $value .= ' 00:00:00';
                    }

                    if ($value[0] === '-' || $value === '') {
                        $value = '0000-00-00 00:00:00';
                    } elseif (\preg_match(
                        '/^([0-9]{2,4}-[0-1][0-9]-[0-3][0-9]|[0-9]+)$/',
                        $value
                    )) {
                        $value = (new \DateTime($value))->format('Y-m-d H:i:s');
                    }
                }

                if ($column instanceof Schema\Column\Date) {
                    if (\strlen($value) === 19) {
                        $value = \substr($value, 0, 10);
                    } elseif ($value[0] === '-' || $value === '') {
                        $value = '0000-00-00';
                    } elseif (\preg_match('/^[0-9]+$/', (string) $value)) {
                        $value = (new \DateTime($value))->format('Y-m-d');
                    }
                }

                if ($column instanceof Schema\Column\Decimal) {
                    return \number_format((float) $value, $column->getDecimalScale(), '.', '');
                }

                return $value;

            case 'float':
                return (float) $value;

            case 'null':
                if ($value === null) {
                    return null;
                }

                return (string) $value;

            default:
                throw new \Exception(
                    "DataIntegrity::coerceValueToSchema found unknown type for field: '{$php_type}'"
                );
        }
    }

    /**
     * @param array<int, array<string, mixed>> $table
     * @param array<string, mixed>             $new_row
     *
     * @return array{0:string, 1:int}|null
     */
    public static function checkUniqueConstraints(
        array $table,
        array $new_row,
        Schema\TableDefinition $table_definition,
        ?int $update_row_id = null
    ) {
        $unique_keys = [];

        foreach ($table_definition->indexes as $name => $index) {
            if ($index->type === 'PRIMARY') {
                $unique_keys['PRIMARY'] = $index->columns;
            } else {
                if ($index->type === 'UNIQUE') {
                    $unique_keys[$name] = $index->columns;
                }
            }
        }

        foreach ($unique_keys as $name => $unique_key) {
            if ($name !== 'PRIMARY') {
                foreach ($unique_key as $key) {
                    if ($new_row[$key] === null) {
                        continue 2;
                    }
                }
            }

            foreach ($table as $row_id => $existing_row) {
                if ($row_id === $update_row_id) {
                    continue;
                }

                $different_keys = array_filter(
                    $unique_key,
                    function ($key) use ($existing_row, $new_row) {
                        return $existing_row[$key] !== $new_row[$key] || !isset($new_row[$key]);
                    }
                );

                // if all keys in the row match
                if (!$different_keys) {
                    $dupe_unique_key_value = \implode(
                        ', ',
                        \array_map(
                            function ($field) use ($existing_row) {
                                return (string) $existing_row[$field];
                            },
                            $unique_key
                        )
                    );
                    return [
                        "Duplicate entry '{$dupe_unique_key_value}' for key"
                            . " '{$name}' in table '{$table_definition->name}'",
                        $row_id
                    ];
                }
            }
        }

        return null;
    }
}

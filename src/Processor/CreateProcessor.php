<?php
namespace Vimeo\MysqlEngine\Processor;

use PhpMyAdmin\SqlParser;

use Vimeo\MysqlEngine\DataType;
use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Schema\Column;

final class CreateProcessor
{
    public static function process(
        \Vimeo\MysqlEngine\FakePdo $conn,
        SqlParser\Statements\CreateStatement $stmt
    ) : void {
        $definition_columns = [];

        $primary_key_columns = [];

        $indexes = [];

        foreach ($stmt->fields as $field) {
            if ($field->key) {
                $columns = \array_map(
                    fn($column) => $column['name'],
                    $field->key->columns
                );

                if ($field->key->type === 'PRIMARY KEY') {
                    $primary_key_columns = $columns;
                }

                $type = \substr($field->key->type, 0, -4);

                $indexes[$field->key->name ?: $type] = new \Vimeo\MysqlEngine\Schema\Index(
                    \substr($field->key->type, 0, -4),
                    $columns
                );
            }

            if (!$field->type || !$field->name) {
                continue;
            }

            $definition_columns[$field->name] = $column = self::getDefinitionColumn($field->type);

            foreach ($field->options->options as $option) {
                if ($option === 'NOT NULL') {
                    $column->isNullable = false;
                } elseif ($option === 'NULL') {
                    $column->isNullable = true;
                } elseif ($option === 'AUTO_INCREMENT' && $column instanceof Column\IntegerColumn) {
                    $column->autoIncrement();
                } elseif (\is_array($option)
                    && $option['name'] === 'DEFAULT'
                    && $column instanceof Column\Defaultable
                ) {
                    $column->setDefault(
                        $option['expr']->column ?? ($option['value'] === 'NULL' ? null : $option['value'])
                    );
                }
            }
        }

        $default_character_set = null;
        $default_collation = null;

        foreach ($stmt->entityOptions->options as $option) {
            if ($option['name'] === 'DEFAULT CHARSET') {
                $default_character_set = $option['value'];
            } elseif ($option['name'] === 'COLLATE') {
                $default_collation = $option['value'];
            }
        }

        if (!$default_collation || !$default_character_set) {
            throw new \UnexpectedValueException('No default collation or character set given');
        }

        $definition = new TableDefinition(
            $stmt->name->table,
            $stmt->name->database ?: $conn->databaseName,
            $definition_columns,
            $default_character_set,
            $default_collation,
            $primary_key_columns,
            $indexes
        );

        $conn->getServer()->addTableDefinition($stmt->name->database ?: $conn->databaseName, $stmt->name->table, $definition);
    }

    private static function getDefinitionColumn(SqlParser\Components\DataType $stmt) : Column
    {
        switch (strtoupper($stmt->name)) {
            case DataType::TINYINT:
            case DataType::SMALLINT:
            case DataType::INT:
            case DataType::BIT:
            case DataType::MEDIUMINT:
            case DataType::BIGINT:
                return self::getIntegerDefinitionColumn($stmt);

            case DataType::FLOAT:
                return new Column\FloatColumn((int) $stmt->parameters[0], (int) $stmt->parameters[1]);

            case DataType::DOUBLE:
                return new Column\DoubleColumn((int) $stmt->parameters[0], (int) $stmt->parameters[1]);

            case DataType::DECIMAL:
                return new Column\Decimal((int) $stmt->parameters[0], (int) $stmt->parameters[1]);

            case DataType::BINARY:
            case DataType::CHAR:
                return new Column\Char((int) $stmt->parameters[0]);

            case DataType::ENUM:
                return new Column\Enum(
                    array_map(
                        fn ($param) => substr($param, 1, -1),
                        $stmt->parameters
                    )
                );

            case DataType::SET:
                return new Column\Set(
                    array_map(
                        fn ($param) => substr($param, 1, -1),
                        $stmt->parameters
                    )
                );

            case DataType::TINYBLOB:
                return new Column\TinyBlob();

            case DataType::BLOB:
                return new Column\Blob();

            case DataType::MEDIUMBLOB:
                return new Column\MediumBlob();

            case DataType::LONGBLOB:
                return new Column\LongBlob();

            case DataType::TEXT:
            case DataType::TINYTEXT:
            case DataType::MEDIUMTEXT:
            case DataType::LONGTEXT:
            case DataType::VARCHAR:
                return self::getTextDefinitionColumn($stmt);

            case DataType::DATE:
                return new Column\Date();

            case DataType::DATETIME:
                return new Column\DateTime();

            case DataType::TIME:
                return new Column\Time();

            case DataType::YEAR:
                return new Column\Year();

            case DataType::TIMESTAMP:
                $timestamp = new Column\Timestamp();
                $timestamp->isNullable = false;
                return $timestamp;

            case DataType::VARBINARY:
                throw new \UnexpectedValueException('VARBINARY is not yet supported');

            case DataType::JSON:
                throw new \UnexpectedValueException('JSON is not yet supported');

            case DataType::NUMERIC:
                throw new \UnexpectedValueException('NUMERIC is not yet supported');

            default:
                throw new \UnexpectedValueException('Column type ' . $stmt->name . ' not recognized');
        }
    }

    private static function getIntegerDefinitionColumn(SqlParser\Components\DataType $stmt)
    {
        $unsigned = false;

        $display_width = (int) $stmt->parameters[0];

        switch (strtoupper($stmt->name)) {
            case DataType::TINYINT:
                return new Column\TinyInt($unsigned, $display_width);

            case DataType::SMALLINT:
                return new Column\SmallInt($unsigned, $display_width);

            case DataType::INT:
                return new Column\IntColumn($unsigned, $display_width);

            case DataType::BIT:
                return new Column\TinyInt($unsigned, $display_width);

            case DataType::MEDIUMINT:
                return new Column\MediumInt($unsigned, $display_width);

            case DataType::BIGINT:
                return new Column\BigInt($unsigned, $display_width);

            default:
                throw new \TypeError('something is bad');
        }
    }

    private static function getTextDefinitionColumn(SqlParser\Components\DataType $stmt)
    {
        $collation = null;
        $character_set = null;

        foreach ($options->options as $option) {
            if (isset($option['name'])) {
                if ($option['name'] === 'CHARACTER SET') {
                    $character_set = $option->value;
                } elseif ($option['name'] === 'COLLATE') {
                    $collation = $option->value;
                }
            }
        }

        switch (strtoupper($stmt->name)) {
            case DataType::TEXT:
                return new Column\Text($character_set, $collation);

            case DataType::TINYTEXT:
                return new Column\TinyText($character_set, $collation);

            case DataType::MEDIUMTEXT:
                return new Column\MediumText($character_set, $collation);

            case DataType::LONGTEXT:
                return new Column\LongText($character_set, $collation);

            case DataType::VARCHAR:
                return new Column\Varchar((int) $stmt->parameters[0], $character_set, $collation);

            default:
                throw new \TypeError('something is bad');
        }
    }
}

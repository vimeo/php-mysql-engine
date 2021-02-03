<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataType;
use Vimeo\MysqlEngine\Query;
use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Schema\Column;

final class CreateProcessor
{
    public static function process(
        \Vimeo\MysqlEngine\FakePdoInterface $conn,
        Query\CreateQuery $stmt
    ) : void {
        $definition_columns = [];

        $primary_key_columns = [];

        $indexes = [];

        foreach ($stmt->fields as $field) {
            $definition_columns[$field->name] = $column = self::getDefinitionColumn($field->type);

            $column->isNullable = (bool) $field->type->null;

            if ($field->auto_increment && $column instanceof Column\IntegerColumn) {
                $column->autoIncrement();
            }

            if ($field->default && $column instanceof Column\Defaultable) {
                $column->setDefault(
                    $field->default === 'NULL' ? null : $field->default
                );
            }
        }

        foreach ($stmt->indexes as $index) {
            $columns = \array_map(
                function ($col) {
                    return $col['name'];
                },
                $index->cols
            );

            if ($index->type === 'PRIMARY') {
                $primary_key_columns = $columns;
            }

            $indexes[$index->name ?: $index->type] = new \Vimeo\MysqlEngine\Schema\Index(
                $index->type,
                $columns
            );
        }


        $default_character_set = null;
        $default_collation = null;

        $auto_increment_offsets = [];

        foreach ($stmt->props as $key => $value) {
            if ($key === 'CHARSET') {
                $default_character_set = $value;
            } elseif ($key === 'COLLATE') {
                $default_collation = $value;
            } elseif ($key === 'AUTO_INCREMENT') {
                foreach ($definition_columns as $name => $column) {
                    if ($column instanceof Column\IntegerColumn && $column->isAutoIncrement()) {
                        $auto_increment_offsets[$name] = $value;
                    }
                }
            }
        }

        if (!$default_collation || !$default_character_set) {
            throw new \UnexpectedValueException('No default collation or character set given');
        }

        $definition = new TableDefinition(
            $stmt->name,
            $conn->getDatabaseName(),
            $definition_columns,
            $default_character_set,
            $default_collation,
            $primary_key_columns,
            $indexes,
            $auto_increment_offsets
        );

        $conn->getServer()->addTableDefinition(
            $conn->getDatabaseName(),
            $stmt->name,
            $definition
        );
    }

    private static function getDefinitionColumn(Query\MysqlColumnType $stmt) : Column
    {
        switch (strtoupper($stmt->type)) {
            case DataType::TINYINT:
            case DataType::SMALLINT:
            case DataType::INT:
            case DataType::BIT:
            case DataType::MEDIUMINT:
            case DataType::BIGINT:
                return self::getIntegerDefinitionColumn($stmt);

            case DataType::FLOAT:
                return new Column\FloatColumn($stmt->length ?? 10, $stmt->decimals ?? 2);

            case DataType::DOUBLE:
                return new Column\DoubleColumn($stmt->length ?? 16, $stmt->decimals ?? 4);

            case DataType::DECIMAL:
                return new Column\Decimal($stmt->length, $stmt->decimals);

            case DataType::BINARY:
            case DataType::CHAR:
                return new Column\Char($stmt->length);

            case DataType::ENUM:
                return new Column\Enum(
                    $stmt->values
                );

            case DataType::SET:
                return new Column\Set(
                    $stmt->values
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
                return new Column\Varbinary((int) $stmt->length);

            case DataType::JSON:
                throw new \UnexpectedValueException('JSON is not yet supported');

            case DataType::NUMERIC:
                throw new \UnexpectedValueException('NUMERIC is not yet supported');

            default:
                throw new \UnexpectedValueException('Column type ' . $stmt->type . ' not recognized');
        }
    }

    /**
     * @return Column\BigInt|Column\IntColumn|Column\MediumInt|Column\SmallInt|Column\TinyInt
     */
    private static function getIntegerDefinitionColumn(Query\MysqlColumnType $stmt)
    {
        $unsigned = $stmt->unsigned;

        $display_width = (int) $stmt->length;

        switch (strtoupper($stmt->type)) {
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

    /**
     * @return Column\LongText|Column\MediumText|Column\Text|Column\TinyText|Column\Varchar
     */
    private static function getTextDefinitionColumn(Query\MysqlColumnType $stmt)
    {
        $collation = null;
        $character_set = null;

        switch (strtoupper($stmt->type)) {
            case DataType::TEXT:
                return new Column\Text($character_set, $collation);

            case DataType::TINYTEXT:
                return new Column\TinyText($character_set, $collation);

            case DataType::MEDIUMTEXT:
                return new Column\MediumText($character_set, $collation);

            case DataType::LONGTEXT:
                return new Column\LongText($character_set, $collation);

            case DataType::VARCHAR:
                return new Column\Varchar((int) $stmt->length, $character_set, $collation);

            default:
                throw new \TypeError('something is bad');
        }
    }
}

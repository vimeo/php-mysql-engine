<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\DataType;
use Vimeo\MysqlEngine\Query;
use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Schema\Column;

final class CreateProcessor
{
    private const CHARSET_MAP = [
        'armscii8' => 'armscii8_general_ci',
        'ascii' => 'ascii_general_ci',
        'big5' => 'big5_chinese_ci',
        'binary' => 'binary',
        'cp1250' => 'cp1250_general_ci',
        'cp1251' => 'cp1251_general_ci',
        'cp1256' => 'cp1256_general_ci',
        'cp1257' => 'cp1257_general_ci',
        'cp850' => 'cp850_general_ci',
        'cp852' => 'cp852_general_ci',
        'cp866' => 'cp866_general_ci',
        'cp932' => 'cp932_japanese_ci',
        'dec8' => 'dec8_swedish_ci',
        'eucjpms' => 'eucjpms_japanese_ci',
        'euckr' => 'euckr_korean_ci',
        'gb18030' => 'gb18030_chinese_ci',
        'gb2312' => 'gb2312_chinese_ci',
        'gbk' => 'gbk_chinese_ci',
        'geostd8' => 'geostd8_general_ci',
        'greek' => 'greek_general_ci',
        'hebrew' => 'hebrew_general_ci',
        'hp8' => 'hp8_english_ci',
        'keybcs2' => 'keybcs2_general_ci',
        'koi8r' => 'koi8r_general_ci',
        'koi8u' => 'koi8u_general_ci',
        'latin1' => 'latin1_swedish_ci',
        'latin2' => 'latin2_general_ci',
        'latin5' => 'latin5_turkish_ci',
        'latin7' => 'latin7_general_ci',
        'macce' => 'macce_general_ci',
        'macroman' => 'macroman_general_ci',
        'sjis' => 'sjis_japanese_ci',
        'swe7' => 'swe7_swedish_ci',
        'tis620' => 'tis620_thai_ci',
        'ucs2' => 'ucs2_general_ci',
        'ujis' => 'ujis_japanese_ci',
        'utf16' => 'utf16_general_ci',
        'utf16le' => 'utf16le_general_ci',
        'utf32' => 'utf32_general_ci',
        'utf8' => 'utf8_general_ci',
        'utf8mb4' => 'utf8mb4_general_ci',
    ];

    public static function makeTableDefinition(
        Query\CreateQuery $stmt,
        string $database_name,
        ?string $default_character_set = null,
        ?string $default_collation = null
    ) : TableDefinition {
        $definition_columns = [];

        $primary_key_columns = [];

        $indexes = [];

        foreach ($stmt->fields as $field) {
            $definition_columns[$field->name] = $column = self::getDefinitionColumn($field->type);

            $column->setNullable((bool) $field->type->null);

            if ($field->auto_increment && $column instanceof Column\IntegerColumn) {
                $column->autoIncrement();
            }

            if ($field->default !== null && $column instanceof Column\Defaultable) {
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

        if (!$default_collation
            && $default_character_set
            && isset(self::CHARSET_MAP[$default_character_set])
        ) {
            $default_collation = self::CHARSET_MAP[$default_character_set];
        }

        if (!$default_collation || !$default_character_set) {
            throw new \UnexpectedValueException('No default collation or character set given');
        }

        return new TableDefinition(
            $stmt->name,
            $database_name,
            $definition_columns,
            $default_character_set,
            $default_collation,
            $primary_key_columns,
            $indexes,
            $auto_increment_offsets
        );
    }

    private static function getDefinitionColumn(Query\MysqlColumnType $stmt) : Column
    {
        switch (strtoupper($stmt->type)) {
            case DataType::TINYINT:
            case DataType::SMALLINT:
            case DataType::INT:
            case DataType::INTEGER:
            case DataType::BIT:
            case DataType::MEDIUMINT:
            case DataType::BIGINT:
                if ($stmt->null === null) {
                    $stmt->null = true;
                }

                return self::getIntegerDefinitionColumn($stmt);

            case DataType::FLOAT:
                return new Column\FloatColumn($stmt->length ?? 10, $stmt->decimals ?? 2);

            case DataType::DOUBLE:
                return new Column\DoubleColumn($stmt->length ?? 16, $stmt->decimals ?? 4);

            case DataType::DECIMAL:
                return new Column\Decimal($stmt->length, $stmt->decimals);

            case DataType::BINARY:
                if ($stmt->length === null) {
                    throw new \UnexpectedValueException('length should not be null');
                }

                return new Column\Binary($stmt->length, 'binary', 'binary');

            case DataType::CHAR:
                if ($stmt->length === null) {
                    throw new \UnexpectedValueException('length should not be null');
                }

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
                if ($stmt->null === null) {
                    $stmt->null = true;
                }

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
                return new Column\Timestamp();

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
            case DataType::INTEGER:
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
        $collation = $stmt->collation;
        $character_set = $stmt->character_set;

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

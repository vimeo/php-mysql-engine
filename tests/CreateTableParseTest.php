<?php
namespace Vimeo\MysqlEngine\Tests;

use Vimeo\MysqlEngine\Parser\CreateTableParser;
use Vimeo\MysqlEngine\Processor\CreateProcessor;
use Vimeo\MysqlEngine\Schema\TableDefinition;

class CreateTableParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = file_get_contents(__DIR__ . '/fixtures/create_table.sql');

        $create_queries = (new CreateTableParser)->parse($query);

        $this->assertNotEmpty($create_queries);

        $table_defs = [];

        foreach ($create_queries as $create_query) {
            $table = CreateProcessor::makeTableDefinition(
                $create_query,
                'foo'
            );
            $table_defs[$table->name] = $table;

            $new_table_php_code = $table->getPhpCode();

            $new_table = eval('return ' . $new_table_php_code . ';');

            $this->assertSame(\var_export($table, true), \var_export($new_table, true),
                "The table definition for {$table->name} did not match the generated php version.");
        }

        // specific parsing checks
        $this->assertInstanceOf(TableDefinition::class, $table_defs['tweets']);
        $this->assertEquals('utf8mb4', $table_defs['tweets']->columns['title']->getCharacterSet());
        $this->assertEquals('utf8mb4_unicode_ci', $table_defs['tweets']->columns['title']->getCollation());
        $this->assertEquals('utf8mb4', $table_defs['tweets']->columns['text']->getCharacterSet());
        $this->assertEquals('utf8mb4_unicode_ci', $table_defs['tweets']->columns['text']->getCollation());

        $this->assertInstanceOf(TableDefinition::class, $table_defs['texts']);
        $this->assertEquals('utf8mb4', $table_defs['texts']->columns['title_char_col']->getCharacterSet());
        $this->assertEquals('utf8mb4_unicode_ci', $table_defs['texts']->columns['title_char_col']->getCollation());
        $this->assertNull($table_defs['texts']->columns['title_col']->getCharacterSet());
        $this->assertEquals('utf8mb4_unicode_ci', $table_defs['texts']->columns['title_col']->getCollation());
        $this->assertNull($table_defs['texts']->columns['title']->getCharacterSet());
        $this->assertNull($table_defs['texts']->columns['title']->getCollation());
    }
}

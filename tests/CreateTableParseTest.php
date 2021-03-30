<?php
namespace Vimeo\MysqlEngine\Tests;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;

class CreateTableParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = file_get_contents(__DIR__ . '/fixtures/create_table.sql');

        $create_queries = (new \Vimeo\MysqlEngine\Parser\CreateTableParser)->parse($query);

        $this->assertCount(4, $create_queries);

        foreach ($create_queries as $create_query) {
            $table = \Vimeo\MysqlEngine\Processor\CreateProcessor::makeTableDefinition(
                $create_query,
                'foo'
            );

            $new_table_php_code = $table->getPhpCode();

            $new_table = eval('return ' . $new_table_php_code . ';');

            $this->assertSame(\var_export($table, true), \var_export($new_table, true));
        }
    }
}

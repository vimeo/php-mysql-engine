<?php
namespace MysqlEngine\Tests;

use MysqlEngine\Query\SelectQuery;
use MysqlEngine\Query\Expression\ColumnExpression;
use MysqlEngine\Query\Expression\CaseOperatorExpression;
use MysqlEngine\Query\Expression\BinaryOperatorExpression;

class CreateTableParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = file_get_contents(__DIR__ . '/fixtures/create_table.sql');

        $create_queries = (new \MysqlEngine\Parser\CreateTableParser)->parse($query);

        $this->assertCount(5, $create_queries);

        foreach ($create_queries as $create_query) {
            $table = \MysqlEngine\Processor\CreateProcessor::makeTableDefinition(
                $create_query,
                'foo'
            );

            $new_table_php_code = $table->getPhpCode();

            $new_table = eval('return ' . $new_table_php_code . ';');

            $this->assertSame(\var_export($table, true), \var_export($new_table, true));
        }
    }
}

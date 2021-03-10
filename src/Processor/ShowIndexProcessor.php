<?php


namespace Vimeo\MysqlEngine\Processor;


use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Query\ShowIndexQuery;
use Vimeo\MysqlEngine\Schema\Column;

class ShowIndexProcessor extends Processor
{
    public static function process(
        FakePdoInterface $conn,
        Scope $scope,
        ShowIndexQuery $stmt
    ): QueryResult {
        [$database, $table] = Processor::parseTableName($conn, $stmt->table);
        $table_definition = $conn->getServer()->getTableDefinition(
            $database,
            $table
        );
        $columns = [
            'Table' => new Column\Varchar(255),
            'Non_unique' => new Column\TinyInt(true, 1),
            'Key_name' => new Column\Varchar(255),
            'Seq_in_index' => new Column\intColumn(true, 4),
            'Column_name' => new Column\Varchar(255),
            'Collation' => new Column\Char(1),
            'Cardinality' => new Column\intColumn(true, 4),
            'Sub_part' => new Column\intColumn(true, 4),
            'Packed' => new Column\TinyInt(true, 1),
            'Null' => new Column\Varchar(3),
            'Index_type' => new Column\Varchar(5),
            'Comment' => new Column\Varchar(255),
            'Index_comment' => new Column\Varchar(255)
        ];
        $rows = [];
        foreach ($table_definition->indexes as $name => $index) {
            foreach ($index->columns as $i => $column) {
                $rows[] = [
                    'Table' => $table_definition->name,
                    'Non_unique' => $index->type === 'INDEX' ? 1 : 0,
                    'Key_name' => $name,
                    'Seq_in_index' => $i + 1,
                    'Column_name' => $column,
                    // Index には "direction" がない(CreateIndex の $cols にはある)ため null
                    'Collation' => null,
                    /*
                     * https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html
                     * ANALYZE TABLE が未実装のため null
                     */
                    'Cardinality' => null,
                    // Index には "length" がない(CreateIndex の $cols にはある)ため null
                    'Sub_part' => null,
                    // PACK_KEYS が未実装のため null
                    'Packed' => null,
                    'Null' => $table_definition->columns[$column]->isNullable ? 'YES' : '',
                    // Index には $mode がない(CreateIndex にはある)ため null
                    'Index_type' => null,
                    // DISABLE KEYS 未実装のため ''
                    'Comment' => '',
                    // CREATE TABLE の INDEX COMMENT がスキップされているので ''
                    'Index_comment' => ''
                ];
            }
        }
        $result = self::applyWhere($conn, $scope, $stmt->whereClause, new QueryResult($rows, $columns));
        return new QueryResult(array_merge($result->rows), $result->columns);
    }
}
<?php


namespace Vimeo\MysqlEngine\Processor;


use Vimeo\MysqlEngine\FakePdoInterface;

class ShowIndexProcessor extends Processor
{
    public static function process(
        FakePdoInterface $conn,
        string $table
    ): array
    {
        $result = [];
        [$database, $table] = Processor::parseTableName($conn, $table);
        $table_definition = $conn->getServer()->getTableDefinition(
            $database,
            $table
        );
        foreach ($table_definition->indexes as $name => $index) {
            foreach ($index->columns as $i => $column) {
                $result[] = [
                    'Table' => $table_definition->name,
                    'Non_unique' => $index->type === 'INDEX' ? 1 : 0,
                    'Key_name' => $name,
                    'Seq_in_index' => $i+1,
                    'Column_name' => $column,
                    /*
                     * https://dev.mysql.com/doc/refman/5.6/ja/create-index.html
                     * index_col_name の指定を ASC または DESC で終了させることができます。
                     * これらのキーワードは、インデックス値の昇順または降順での格納を指定する将来の拡張のために許可されています。
                     * 現在、これらは解析されますが、無視されます。インデックス値は、常に昇順で格納されます。
                     */
                    'Collation' => 'A',
                    /*
                     * https://dev.mysql.com/doc/refman/5.6/ja/analyze-table.html
                     * ANALYZE TABLE が未実装のため null
                     */
                    'Cardinality' => null,
                    //  Index には "length" がない(CreateIndex の $cols にはある)ため null
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
        return $result;
    }
}
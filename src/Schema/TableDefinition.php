<?php
namespace Vimeo\MysqlEngine\Schema;

class TableDefinition
{
    public string $name;

    public string $databaseName;

    public string $defaultCharacterSet;

    public string $defaultCollation;

    /**
     * @var array<string, Column>
     */
    public array $columns;

    public array $primaryKeyColumns;

    public array $indexes;

    /**
     * @var array<string, int>
     */
    public array $autoIncrementOffsets = [];

    public function __construct(
        string $name,
        string $databaseName,
        array $columns,
        string $characterSet,
        string $collation,
        array $primaryKeyColumns,
        array $indexes
    ) {
        $this->name = $name;
        $this->databaseName = $databaseName;
        $this->columns = $columns;
        $this->defaultCharacterSet = $characterSet;
        $this->defaultCollation = $collation;
        $this->primaryKeyColumns = $primaryKeyColumns;
        $this->indexes = $indexes;
    }
}

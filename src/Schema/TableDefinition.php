<?php
namespace MysqlEngine\Schema;

class TableDefinition
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var string
     */
    public $databaseName;

    /**
     * @var string
     */
    public $defaultCharacterSet;

    /**
     * @var string
     */
    public $defaultCollation;

    /**
     * @var array<string, Column>
     */
    public $columns;

    /**
     * @var array<string>
     */
    public $primaryKeyColumns;

    /**
     * @var array<string, Index>
     */
    public $indexes;

    /**
     * @var array<string, int>
     */
    public $autoIncrementOffsets = [];

    /**
     * @param array<string, Column> $columns
     * @param array<string, Index> $indexes
     * @param array<string> $primaryKeyColumns
     */
    public function __construct(
        string $name,
        string $databaseName,
        array $columns,
        string $characterSet = '',
        string $collation = '',
        array $primaryKeyColumns = [],
        array $indexes = [],
        array $autoIncrementOffsets = []
    ) {
        $this->name = $name;
        $this->databaseName = $databaseName;
        $this->columns = $columns;
        $this->defaultCharacterSet = $characterSet;
        $this->defaultCollation = $collation;
        $this->primaryKeyColumns = $primaryKeyColumns;
        $this->indexes = $indexes;
        $this->autoIncrementOffsets = $autoIncrementOffsets;
    }

    public function getPhpCode() : string
    {
        $columns = [];

        foreach ($this->columns as $name => $column) {
            $columns[] = '\'' . $name . '\' => ' . $column->getPhpCode();
        }

        $indexes = [];

        foreach ($this->indexes as $name => $index) {
            $indexes[] = '\'' . $name . '\' => new \\'
                . \get_class($index) . '(\'' . $index->type
                . '\', [\'' . \implode('\', \'', $index->columns) . '\'])';
        }

        return 'new \\' . self::class . '('
            . '\'' . $this->name . '\''
            . ', \'' . $this->databaseName . '\''
            . ', [' . \implode(', ', $columns) . ']'
            . ', \'' . $this->defaultCharacterSet . '\''
            . ', \'' . $this->defaultCollation . '\''
            . ', ['
            . ($this->primaryKeyColumns ? '\'' . \implode('\', \'', $this->primaryKeyColumns) . '\'' : '')
            . ']'
            . ', [' . \implode(', ', $indexes) . ']'
            . ', ' . \var_export($this->autoIncrementOffsets, true)
            . ')';
    }
}

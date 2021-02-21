<?php
namespace Vimeo\MysqlEngine;

final class Server
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var ?ServerConfig
     */
    public $config = null;

    /**
     * @var array<string, Server>
     */
    private static $instances = [];

    /**
     * @var array<string, true>
     */
    private static $snapshot_names = [];

    /**
     * @var array<string, array<string, TableData>>
     */
    public $databases = [];

    /**
     * @var array<string, array<string, array<string, TableData>>>
     */
    private $snapshots = [];

    /**
     * @var array<string, array<string, Schema\TableDefinition>>
     */
    private $tableDefinitions = [];

    /**
     * @param ?ServerConfig|null $config
     */
    public function __construct(string $name, ?ServerConfig $config = null)
    {
        $this->name = $name;
        $this->config = $config;
    }

    /**
     * @return array<string, self>
     */
    public static function getAll() : array
    {
        return static::$instances;
    }

    public static function get(string $name) : ?self
    {
        return static::$instances[$name] ?? null;
    }

    /**
     * @param ServerConfig $config
     */
    public function setConfig(ServerConfig $config) : void
    {
        $this->config = $config;
    }

    public static function getOrCreate(string $name) : self
    {
        $server = static::$instances[$name] ?? null;

        if ($server === null) {
            $server = new static($name);
            static::$instances[$name] = $server;
        }

        return $server;
    }

    public static function reset() : void
    {
        foreach (static::getAll() as $server) {
            $server->doReset();
        }
    }

    public static function snapshot(string $name) : void
    {
        foreach (static::getAll() as $server) {
            $server->doSnapshot($name);
        }

        static::$snapshot_names[$name] = true;
    }

    public static function restoreSnapshot(string $name) : void
    {
        if (!static::hasSnapshot($name)) {
            throw new Processor\ProcessorException("Snapshot {$name} not found, unable to restore");
        }

        foreach (static::getAll() as $server) {
            $server->doRestoreSnapshot($name);
        }

        unset(static::$snapshot_names[$name]);
    }

    public static function deleteSnapshot(string $name) : bool
    {
        if (!static::hasSnapshot($name)) {
            return false;
        }

        foreach (static::getAll() as $server) {
            $server->doDeleteSnapshot($name);
        }

        unset(static::$snapshot_names[$name]);

        return true;
    }

    public static function haveSnapshot(string $name) : bool
    {
        return \array_key_exists($name, static::$snapshot_names);
    }

    protected function doSnapshot(string $name) : void
    {
        $this->snapshots[$name] = array_map(
            function ($database) {
                return array_map(
                    function ($table) {
                        return clone $table;
                    },
                    $database
                );
            },
            $this->databases
        );
    }

    protected function doDeleteSnapshot(string $name) : void
    {
        unset($this->snapshots[$name]);
    }

    protected function doRestoreSnapshot(string $name) : void
    {
        $this->databases = $this->snapshots[$name] ?? [];
        unset($this->snapshots[$name]);
    }

    protected function doReset() : void
    {
        $this->databases = [];
    }

    public function addTableDefinition(
        string $database,
        string $table,
        Schema\TableDefinition $table_definition
    ) : void {
        $this->tableDefinitions[$database][$table] = $table_definition;
    }

    public function getTableDefinition(string $database, string $table) : ?Schema\TableDefinition
    {
        return $this->tableDefinitions[$database][$table] ?? null;
    }

    /**
     * @return array<int, array<string, mixed>>|null
     */
    public function getTable(string $database_name, string $name) : ?array
    {
        return $this->databases[$database_name][$name]->table ?? null;
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     */
    public function saveTable(string $database_name, string $name, array $rows) : void
    {
        if (!isset($this->databases[$database_name][$name])) {
            $this->databases[$database_name][$name] = new TableData();
        }

        $this->databases[$database_name][$name]->table = $rows;
    }

    public function resetTable(string $database_name, string $name) : void
    {
        static::$snapshot_names = [];
        $this->snapshots = [];
        $this->databases[$database_name][$name] = new TableData();
        $this->databases[$database_name][$name]->was_truncated = true;
    }

    public function dropTable(string $database_name, string $name) : void
    {
        static::$snapshot_names = [];
        $this->snapshots = [];
        unset($this->databases[$database_name][$name]);
    }

    public function getNextAutoIncrementValue(
        string $database_name,
        string $table_name,
        string $column_name
    ) : int {
        $table_definition = $this->getTableDefinition($database_name, $table_name);

        $table = $this->databases[$database_name][$table_name] ?? null;

        if (!$table_definition) {
            throw new \UnexpectedValueException('table doesn’t exist');
        }

        if (!$table) {
            $table = $this->databases[$database_name][$table_name] = new TableData();
        }

        if (!isset($table->autoIncrementCursors[$column_name])) {
            if (isset($table_definition->autoIncrementOffsets[$column_name]) && !$table->was_truncated) {
                $table->autoIncrementCursors[$column_name] = $table_definition->autoIncrementOffsets[$column_name] - 1;
            } else {
                $table->autoIncrementCursors[$column_name] = 0;
            }
        }

        return $table->autoIncrementCursors[$column_name] + 1;
    }

    public function addAutoIncrementMinValue(
        string $database_name,
        string $table_name,
        string $column_name,
        int $value
    ) : void {
        $table_definition = $this->getTableDefinition($database_name, $table_name);
        $table = $this->databases[$database_name][$table_name] ?? null;

        if (!$table_definition) {
            throw new \UnexpectedValueException('table doesn’t exist');
        }

        if (!$table) {
            $table = $this->databases[$database_name][$table_name] = new TableData();
        }

        $table->autoIncrementCursors[$column_name] = max(
            $table->autoIncrementCursors[$column_name] ?? 0,
            $value
        );
    }
}

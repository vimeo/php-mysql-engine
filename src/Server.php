<?php
namespace Vimeo\MysqlEngine;

final class Server
{
    public string $name;

    /**
     * @var array{mysql_version:string, is_vitess:bool, strict_sql_mode:bool, strict_schema_mode:bool, inherit_schema_from:string}|null
     */
    public ?array $config = null;

    /**
     * @var array<string, Server>
     */
    private static array $instances = [];

    /**
     * @var array<string, string>
     */
    private static array $snapshot_names = [];

    /**
     * @var array<string, array<string, TableData>>
     */
    public array $databases = [];

    /**
     * @var array<string, array<string, array<string, TableData>>>
     */
    private array $snapshots = [];

    /**
     * @var array<string, array<string, Schema\TableDefinition>>
     */
    private array $tableDefinitions = [];

    /**
     * @param array{mysql_version:string, is_vitess:bool, strict_sql_mode:bool, strict_schema_mode:bool, inherit_schema_from:string}|null $config
     */
    public function __construct(string $name, ?array $config = null)
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
     * @param array{mysql_version:string, is_vitess:bool, strict_sql_mode:bool, strict_schema_mode:bool, inherit_schema_from:string} $config
     */
    public function setConfig(array $config) : void
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
        if (!\array_key_exists($name, static::$snapshot_names)) {
            throw new Processor\SQLFakeRuntimeException("Snapshot {$name} not found, unable to restore");
        }

        foreach (static::getAll() as $server) {
            $server->doRestoreSnapshot($name);
        }

        unset(static::$snapshot_names[$name]);
    }

    public static function deleteSnapshot(string $name) : bool
    {
        if (!\array_key_exists($name, static::$snapshot_names)) {
            return false;
        }

        foreach (static::getAll() as $server) {
            $server->doDeleteSnapshot($name);
        }

        unset(static::$snapshot_names[$name]);

        return true;
    }

    protected function doSnapshot(string $name) : void
    {
        $this->snapshots[$name] = array_map(
            fn($database) => array_map(
                fn($table) => clone $table,
                $database
            ),
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

    public function addTableDefinition(string $database, string $table, Schema\TableDefinition $table_definition) : void
    {
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

    /**
     * @param array<int, array<string, mixed>> $rows
     */
    public function resetTable(string $database_name, string $name) : void
    {
        static::$snapshot_names = [];
        $this->snapshots = [];
        $this->databases[$database_name][$name] = new TableData();
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
            // todo use table default
            $table->autoIncrementCursors[$column_name] = 0;
        }

        $next = $table->autoIncrementCursors[$column_name];

        do {
            ++$next;
        } while (\array_key_exists($next, $table->autoIncrementIndexes[$column_name]));

        $table->autoIncrementCursors[$column_name] = $next;

        //var_dump('`' . $table_name . '`.`' . $column_name . '` next ' . $next);

        return $next;
    }

    public function addAutoIncrementMinValue(
        string $database_name,
        string $table_name,
        string $column_name,
        int $value
    ) : int {
        $table_definition = $this->getTableDefinition($database_name, $table_name);
        $table = $this->databases[$database_name][$table_name] ?? null;

        if (!$table_definition) {
            throw new \UnexpectedValueException('table doesn’t exist');
        }

        if (!$table) {
            $table = $this->databases[$database_name][$table_name] = new TableData();
        }

        return $table->autoIncrementIndexes[$column_name][$value] = true;
    }
}


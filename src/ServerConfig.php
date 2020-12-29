<?php
namespace Vimeo\MysqlEngine;

final class ServerConfig
{
    public string $mysqlVersion;

    public bool $isVitess;

    public bool $strictSqlMode;

    public bool $strictSchemaMode;

    public string $insertSchemaFrom;

    public function __construct(
        string $mysqlVersion,
        bool $isVitess,
        bool $strictSqlMode,
        bool $strictSchemaMode,
        string $insertSchemaFrom
    ) {
        $this->mysqlVersion = $mysqlVersion;
        $this->isVitess = $isVitess;
        $this->strictSqlMode = $strictSqlMode;
        $this->strictSchemaMode = $strictSchemaMode;
        $this->insertSchemaFrom = $insertSchemaFrom;
    }
}

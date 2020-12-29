<?php
namespace Vimeo\MysqlEngine;

final class ServerConfig
{
    /**
     * @var string
     */
    public $mysqlVersion;

    /**
     * @var bool
     */
    public $isVitess;

    /**
     * @var bool
     */
    public $strictSqlMode;

    /**
     * @var bool
     */
    public $strictSchemaMode;

    /**
     * @var string
     */
    public $insertSchemaFrom;

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

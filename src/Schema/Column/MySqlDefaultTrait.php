<?php

namespace MysqlEngine\Schema\Column;

/**
 * Trait MySqlDefaultTrait
 * @package MysqlEngine\Schema\Column
 */
trait MySqlDefaultTrait
{
    /**
     * @var ?string
     */
    protected $mysqlDefault;

    /**
     * @var bool
     */
    protected $hasMysqlDefault = false;

    /**
     * @param string|null $mysqlDefault
     * @return static
     */
    public function setDefault(?string $mysqlDefault)
    {
        $this->mysqlDefault = $mysqlDefault;
        $this->hasMysqlDefault = true;

        return $this;
    }

    /**
     * @return bool
     */
    public function hasDefault(): bool
    {
        return $this->hasMysqlDefault;
    }

    /**
     * @return string|null
     */
    public function getDefault(): ?string
    {
        return $this->mysqlDefault;
    }
}

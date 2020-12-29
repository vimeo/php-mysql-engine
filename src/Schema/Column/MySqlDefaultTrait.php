<?php
namespace Vimeo\MysqlEngine\Schema\Column;

trait MySqlDefaultTrait
{
    /**
     * @var mixed
     */
    protected $mysql_default = null;

    /**
     * @var bool
     */
    protected $has_mysql_default = false;

    public function setDefault($mysql_default) : void
    {
        $this->mysql_default = $mysql_default;
        $this->has_mysql_default = true;
    }

    public function hasDefault() : bool
    {
        return $this->has_mysql_default;
    }

    /**
     * @return mixed
     */
    public function getDefault()
    {
        return $this->mysql_default;
    }
}

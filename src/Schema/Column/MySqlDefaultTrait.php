<?php
namespace Vimeo\MysqlEngine\Schema\Column;

trait MySqlDefaultTrait
{
    /** @var mixed */
    protected $mysql_default = null;

    /** @var bool */
    protected $has_mysql_default = false;

    /**
     * @return $this
     */
    public function setDefault($mysql_default)
    {
        $this->mysql_default = $mysql_default;
        $this->has_mysql_default = true;
        return $this;
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

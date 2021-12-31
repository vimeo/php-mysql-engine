<?php
namespace MysqlEngine\Schema\Column;

trait NumberColumnTrait
{
    /**
     * @var bool
     */
    protected $unsigned = false;

    public function isUnsigned() : bool
    {
        return $this->unsigned;
    }
}

<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface NumberColumn
{
	/**
     * @return numeric
     */
    public function getMaxValue();

    /**
     * @return numeric
     */
    public function getMinValue();

    /**
     * @return $this
     */
    public function setDefault($mysql_default);
}

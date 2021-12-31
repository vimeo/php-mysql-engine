<?php
namespace MysqlEngine\Schema\Column;

class TinyInt extends \MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, DefaultTable
{
    use IntegerColumnTrait;
    use MySqlDefaultTrait;

    /**
     * @return int
     */
    public function getMaxValue()
    {
        if ($this->unsigned) {
            return 255;
        }

        return 127;
    }

    /**
     * @return int
     */
    public function getMinValue()
    {
        if ($this->unsigned) {
            return 0;
        }

        return -128;
    }
}

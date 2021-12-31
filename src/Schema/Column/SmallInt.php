<?php
namespace MysqlEngine\Schema\Column;

class SmallInt extends \MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, DefaultTable
{
    use IntegerColumnTrait;
    use MySqlDefaultTrait;

    /**
     * @return int
     */
    public function getMaxValue()
    {
        if ($this->unsigned) {
            return 65535;
        }

        return 32767;
    }

    /**
     * @return int
     */
    public function getMinValue()
    {
        if ($this->unsigned) {
            return 0;
        }

        return -32768;
    }
}

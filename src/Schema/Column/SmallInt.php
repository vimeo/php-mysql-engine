<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class SmallInt extends \Vimeo\MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, Defaultable
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

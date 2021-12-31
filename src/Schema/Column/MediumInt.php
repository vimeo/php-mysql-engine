<?php
namespace MysqlEngine\Schema\Column;

class MediumInt extends \MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, DefaultTable
{
    use IntegerColumnTrait;
    use MySqlDefaultTrait;

    /**
     * @return int
     */
    public function getMaxValue()
    {
        if ($this->unsigned) {
            return 16777215;
        }

        return 8388607;
    }

    /**
     * @return int
     */
    public function getMinValue()
    {
        if ($this->unsigned) {
            return 0;
        }

        return -8388608;
    }
}

<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class IntColumn extends \Vimeo\MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, Defaultable
{
    use IntegerColumnTrait;
    use MySqlDefaultTrait;

    public function getMaxValue()
    {
        if ($this->unsigned) {
            return 4294967295;
        }

        return 2147483647;
    }

    public function getMinValue()
    {
        if ($this->unsigned) {
            return 0;
        }

        return -2147483648;
    }
}

<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class TinyInt extends \Vimeo\MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, Defaultable
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

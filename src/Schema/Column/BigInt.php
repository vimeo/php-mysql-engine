<?php
namespace MysqlEngine\Schema\Column;

class BigInt extends \MysqlEngine\Schema\Column implements NumberColumn, IntegerColumn, DefaultTable
{
    use IntegerColumnTrait;
    use MySqlDefaultTrait;

    /**
     * @return int|float
     */
    public function getMaxValue()
    {
        if ($this->unsigned) {
            return 18446744073709551615;
        }

        return 9223372036854775807;
    }

    /**
     * @return float|int
     */
    public function getMinValue()
    {
        if ($this->unsigned) {
            return 0;
        }

        return -9223372036854775808;
    }
}

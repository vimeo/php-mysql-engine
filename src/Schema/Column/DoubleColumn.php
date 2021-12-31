<?php
namespace MysqlEngine\Schema\Column;

class DoubleColumn extends DecimalPointColumn implements DefaultTable
{
    use NumberColumnTrait;
    use MySqlDefaultTrait;
}

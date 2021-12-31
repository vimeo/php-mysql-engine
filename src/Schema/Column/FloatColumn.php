<?php
namespace MysqlEngine\Schema\Column;

class FloatColumn extends DecimalPointColumn implements DefaultTable
{
    use NumberColumnTrait;
}

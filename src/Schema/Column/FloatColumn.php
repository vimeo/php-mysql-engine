<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class FloatColumn extends DecimalPointColumn implements Defaultable
{
    use NumberColumnTrait;
}

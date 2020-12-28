<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class DoubleColumn extends DecimalPointColumn implements Defaultable
{
    use NumberColumnTrait;
    use MySqlDefaultTrait;
}

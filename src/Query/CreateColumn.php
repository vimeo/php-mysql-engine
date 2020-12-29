<?php
namespace Vimeo\MysqlEngine\Query;

class CreateColumn
{
    public string $name;

    public string $type;

    public int $length;

    public int $decimals;

    public bool $unsigned;

    public bool $null;

    public string $default;

    public array $values;

    public bool $auto_increment;

    public bool $zerofill = false;

    public string $character_set;

    public string $collation;

    public $more;
}

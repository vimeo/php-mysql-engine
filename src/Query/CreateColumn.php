<?php
namespace MysqlEngine\Query;

class CreateColumn
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var MysqlColumnType
     */
    public $type;

    /**
     * @var ?string
     */
    public $default;

    /**
     * @var ?bool
     */
    public $auto_increment;

    /**
     * @var ?list<string>
     */
    public $more;

    public function __construct(string $name, MysqlColumnType $type)
    {
        $this->name = $name;
        $this->type = $type;
    }
}

<?php
namespace MysqlEngine\Query;

class CreateQuery
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var array<int, CreateColumn>
     */
    public $fields = [];

    /**
     * @var string
     */
    public $sql;
    
    /**
     * @var array<int, CreateIndex>
     */
    public $indexes = [];
    
    /**
     * @var array<string, string>
     */
    public $props;
}

<?php
namespace Vimeo\MysqlEngine\Query;

class CreateQuery
{
    public string $name;

    /**
     * @var array<int, CreateColumn>
     */
    public array $fields = [];

    public string $sql;
    
    /**
     * @var array<int, CreateIndex>
     */
    public array $indexes = [];
    
    /**
     * @var array<string, string>
     */
    public array $props;
}

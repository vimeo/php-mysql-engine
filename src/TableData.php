<?php
namespace MysqlEngine;

class TableData
{
    /**
     * @var array<int, array<string, mixed>>
     */
    public $table = [];

    /**
     * @var array<string, int>
     */
    public $autoIncrementCursors = [];

    /**
     * @var bool
     */
    public $was_truncated = false;
}

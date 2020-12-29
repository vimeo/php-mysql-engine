<?php
namespace Vimeo\MysqlEngine;

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
     * @var array<string, array<int, true>>
     */
    public $autoIncrementIndexes = [];
}

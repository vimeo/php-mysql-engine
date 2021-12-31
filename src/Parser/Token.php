<?php
namespace MysqlEngine\Parser;

use MysqlEngine\TokenType;

class Token
{
    /**
     * @var TokenType::*
     */
    public $type;

    /**
     * @var string
     */
    public $value;

    /**
     * @var string
     */
    public $raw;

    /**
     * @var int
     */
    public $start;

    /**
     * @var ?string
     */
    public $parameterName;

    /**
     * @var ?int
     */
    public $parameterOffset;

    /**
     * Token constructor.
     * @param TokenType::* $type
     * @param string $value
     * @param string $raw
     * @param int $start
     */
    public function __construct(
        string $type,
        string $value,
        string $raw,
        int $start
    ) {
        $this->type = $type;
        $this->value = $value;
        $this->raw = $raw;
        $this->start = $start;
    }
}

<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;

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
     * @param TokenType::* $type
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

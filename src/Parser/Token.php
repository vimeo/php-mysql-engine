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
     * @param TokenType::* $type
     */
    public function __construct(
        string $type,
        string $value,
        string $raw
    ) {
        $this->type = $type;
        $this->value = $value;
        $this->raw = $raw;
    }
}

<?php
namespace Vimeo\MysqlEngine\Schema\Column;

abstract class CharacterColumn extends \Vimeo\MysqlEngine\Schema\Column
{
    /**
     * @var int
     */
    protected $max_string_length;

    /**
     * @var ?int
     */
    protected $max_truncated_length; // used for in-memory columns

    /**
     * @var ?string
     */
    protected $character_set;

    /**
     * @var ?string
     */
    protected $collation;

    public function __construct(int $max_string_length, ?string $character_set = null, ?string $collation = null)
    {
        $this->max_string_length = $max_string_length;
        $this->character_set = $character_set;
        $this->collation = $collation;
    }

    public function getMaxStringLength() : int
    {
        return $this->max_string_length;
    }

    public function getMaxTruncatedStringLength() : ?int
    {
        return $this->max_truncated_length;
    }

    public function getCharacterSet() : ?string
    {
        return $this->character_set;
    }

    public function getCollation() : ?string
    {
        return $this->collation;
    }

    public function getPhpType() : string
    {
        return 'string';
    }

    public function getPhpCode() : string
    {
        $default = '';

        if ($this instanceof Defaultable && $this->hasDefault()) {
            if ($this->getDefault() === null) {
                $default = '->setDefault(null)';
            } else {
                $default = '->setDefault(\'' . $this->getDefault() . '\')';
            }
        }

        $args = [
            $this->max_string_length,
            $this->character_set === null ? 'null' : "'{$this->character_set}'",
            $this->collation === null ? 'null' : "'{$this->collation}'",
        ];

        return '(new \\' . static::class . '(' . implode(', ', $args) . '))'
            . $default
            . $this->getNullablePhp();
    }
}

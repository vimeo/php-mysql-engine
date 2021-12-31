<?php

namespace MysqlEngine\Schema\Column;

abstract class CharacterColumn extends \MysqlEngine\Schema\Column
{
    /**
     * @var int
     */
    protected $maxStringLength;

    /**
     * @var ?int
     */
    protected $maxTruncatedLength; // used for in-memory columns

    /**
     * @var ?string
     */
    protected $characterSet;

    /**
     * @var ?string
     */
    protected $collation;

    public function __construct(int $max_string_length, ?string $character_set = null, ?string $collation = null)
    {
        $this->maxStringLength = $max_string_length;
        $this->characterSet = $character_set;
        $this->collation = $collation;
    }

    public function getMaxStringLength(): int
    {
        return $this->maxStringLength;
    }

    public function getMaxTruncatedStringLength(): ?int
    {
        return $this->maxTruncatedLength;
    }

    public function getCharacterSet(): ?string
    {
        return $this->characterSet;
    }

    public function getCollation(): ?string
    {
        return $this->collation;
    }

    public function getPhpType(): string
    {
        return 'string';
    }

    public function getPhpCode(): string
    {
        $default = '';

        if ($this instanceof DefaultTable && $this->hasDefault()) {
            $mysqlDefault = $this->getDefault();
            $default = $mysqlDefault === null
                ? '->setDefault(null)'
                : ('->setDefault(\'' . $mysqlDefault . '\')');
        }

        return '(new \\' . static::class . '('
            . $this->maxStringLength
            . ($this->characterSet !== null && $this->collation !== null
                ? ', \'' . $this->characterSet . '\'' . ', \'' . $this->collation . '\''
                : '')
            . '))'
            . $default
            . $this->getNullablePhp();
    }
}

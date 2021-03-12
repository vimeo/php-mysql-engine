<?php
namespace Vimeo\MysqlEngine\Schema\Column;

trait HasOptionsTrait
{
    /**
     * @var string[]
     */
    protected $options = [];

    /**
     * @param string[] $options
     */
    public function __construct(array $options)
    {
        $this->options = $options;
    }

    /**
     * @return       string[]
     * @psalm-return array<mixed, string>
     */
    public function getOptions() : array
    {
        return $this->options;
    }

    /**
     * @return 'int'|'string'|'float'|'null'
     */
    public function getPhpType() : string
    {
        return 'string';
    }

    public function getPhpCode() : string
    {
        $default = $this->getDefault() !== null ? '\'' . $this->getDefault() . '\'' : 'null';
        
        return '(new \\' . static::class . '([\'' . \implode('\', \'', $this->options) . '\']))'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}

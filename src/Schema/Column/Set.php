<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Set extends \Vimeo\MysqlEngine\Schema\Column implements Defaultable
{
    use MySqlDefaultTrait;

    /** @var string[] */
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

    public function getPhpType() : string
    {
        return 'string';
    }
}

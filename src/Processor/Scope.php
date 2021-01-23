<?php
namespace Vimeo\MysqlEngine\Processor;

class Scope
{
	/**
	 * @var array<string, mixed>
	 */
	public $variables = [];

    /**
     * @var array<string, mixed>
     */
    public $parameters = [];

    public function __construct(array $parameters)
    {
        $this->parameters = $parameters;
    }
}

<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;

class Token
{
	/**
	 * @var TokenType::*
	 */
	public string $type;

	public string $value;

	public string $raw;

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

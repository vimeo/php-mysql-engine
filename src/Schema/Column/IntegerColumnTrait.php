<?php
namespace Vimeo\MysqlEngine\Schema\Column;

trait IntegerColumnTrait
{
    /**
     * @var bool
     */
    protected $auto_increment = false;

    /**
     * @var int
     */
    protected $integer_display_width;

    /**
     * @var bool
     */
    protected $unsigned = false;

    public function __construct(bool $unsigned, int $integer_display_width)
    {
        $this->unsigned = $unsigned;
        $this->integer_display_width = $integer_display_width;
    }

    public function getDisplayWidth() : int
    {
        return $this->integer_display_width;
    }

    public function autoIncrement() : void
    {
        $this->auto_increment = true;
    }

    public function isAutoIncrement() : bool
    {
        return $this->auto_increment;
    }

    public function isUnsigned() : bool
    {
        return $this->unsigned;
    }

    /**
     * @return 'int'
     */
    public function getPhpType() : string
    {
        return 'int';
    }
}

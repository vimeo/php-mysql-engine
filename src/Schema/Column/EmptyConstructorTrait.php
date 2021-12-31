<?php
namespace MysqlEngine\Schema\Column;

/**
 * Trait EmptyConstructorTrait
 * @package MysqlEngine\Schema\Column
 */
trait EmptyConstructorTrait
{
    /**
     * @return string
     */
    public function getPhpCode() : string
    {
        $default = '';
        /**
         * @psalm-suppress UndefinedMethod
         * @psalm-suppress MixedAssignment
         */
        $mysqlDefault = $this->getDefault();
        if ($this instanceof DefaultTable && $this->hasDefault()) {
            /**
             * @psalm-suppress MixedOperand
             */
            $default = $mysqlDefault === null ? '->setDefault(null)' : '->setDefault(\'' . $mysqlDefault . '\')';
        }

        return '(new \\' . static::class . '())'
            . $default
            . $this->getNullablePhp();
    }
}

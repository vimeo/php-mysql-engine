<?php
namespace Vimeo\MysqlEngine\Php8;

class FakePdo extends \PDO implements \Vimeo\MysqlEngine\FakePdoInterface
{
    use \Vimeo\MysqlEngine\FakePdoTrait;

    /**
     * @param  string $statement
     * @param  ?array $options
     */
    public function prepare($statement, $options = null)
    {
        return new FakePdoStatement($this, $statement, $this->real);
    }
}

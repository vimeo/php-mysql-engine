<?php
namespace Vimeo\MysqlEngine\Php7;

use PDO;
use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\FakePdoTrait;

class FakePdo extends PDO implements FakePdoInterface
{
    use FakePdoTrait;

    /**
     * @param string $statement
     * @param array $options
     * @return FakePdoStatement
     */
    public function prepare($statement, $options = [])
    {
        $stmt = new FakePdoStatement($this, $statement, $this->real);
        if ($this->default_fetch_mode) {
            $stmt->setFetchMode($this->default_fetch_mode);
        }
        return $stmt;
    }

    /**
     * @param string $statement
     * @param int $mode
     * @param null $arg3
     * @param array $ctorargs
     * @return FakePdoStatement
     */
    public function query($statement, $mode = PDO::ATTR_DEFAULT_FETCH_MODE, $arg3 = null, array $ctorargs = [])
    {
        $sth = $this->prepare($statement);
        $sth->execute();
        return $sth;
    }
}

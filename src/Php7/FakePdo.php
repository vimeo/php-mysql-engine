<?php
namespace Vimeo\MysqlEngine\Php7;

use PDO;
use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\FakePdoTrait;

class FakePdo extends PDO implements FakePdoInterface
{
    use FakePdoTrait;

    /**
     * @param  string $statement
     * @param  ?array $options
     */
    public function prepare($statement, array $options = [])
    {
        return new FakePdoStatement($this, $statement, $this->real);
    }

	/**
	 * @param string $statement
	 * @return int|false
	 */
	public function exec($statement)
	{
		$statement = trim($statement);
		if (strpos($statement, 'SET ')===0){
			return false;
		}

		$sth = $this->prepare($statement);
		if ($sth->execute()){
			return $sth->rowCount();
		}
		return false;
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

<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;

final class ColumnExpression extends Expression
{
    /**
     * @var string
     */
    public $columnExpression;

    /**
     * @var string
     */
    public $columnName;

    /**
     * @var null|string
     */
    public $tableName;

    /**
     * @var null|string
     */
    public $databaseName;

    /**
     * @var bool
     */
    public $allowFallthrough = false;

    /**
     * @param Token $token
     */
    public function __construct(Token $token)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->columnExpression = $token->value;
        $this->columnName = $token->value;
        if (\strpos($token->value, '.') !== false) {
            $parts = \explode('.', $token->value);
            if (\count($parts) === 2) {
                list($this->tableName, $this->columnName) = $parts;
            } else {
                if (\count($parts) === 3) {
                    list($this->databaseName, $this->tableName, $this->columnName) = $parts;
                }
            }
        } else {
            $this->tableName = null;
        }
        if ($token->value === '*') {
            $this->name = '*';
            return;
        }

        if ($this->columnName[0] === '`') {
            $this->columnName = \substr($this->columnName, 1, -1);
        }

        if ($this->tableName !== null && $this->tableName[0] === '`') {
            $this->tableName = \substr($this->tableName, 1, -1);
        }

        $this->name = $this->columnName;
    }

    /**
     * @return void
     */
    public function allowFallthrough()
    {
        $this->allowFallthrough = true;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }

    /**
     * @return null|string
     */
    public function tableName()
    {
        return $this->tableName;
    }
}

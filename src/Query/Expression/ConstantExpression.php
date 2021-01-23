<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\ProcessorException;

final class ConstantExpression extends Expression
{
    /**
     * @var scalar
     */
    public $value;

    /**
     * @param Token $token
     */
    public function __construct(Token $token)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->name = $token->value;
        $this->value = self::extractConstantValue($token);
        $this->start = $token->start;
    }

    /**
     * @param Token $token
     *
     * @return null|scalar
     */
    private static function extractConstantValue(Token $token)
    {
        switch ($token->type) {
            case TokenType::NUMERIC_CONSTANT:
                if (\strpos($token->value, '.') !== false) {
                    return (double) $token->value;
                }

                return (int) $token->value;

            case TokenType::STRING_CONSTANT:
                return $token->value;

            case TokenType::NULL_CONSTANT:
                return null;

            default:
                throw new ProcessorException(
                    "Attempted to assign invalid token type {$token->type} to Constant Expression"
                );
        }
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}

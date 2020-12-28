<?php
namespace Vimeo\MysqlEngine\Query\Expression;


use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;

final class ConstantExpression extends Expression
{
    /**
     * @var scalar
     */
    public $value;

    /**
     * @param array{type: TokenType::*, value: scalar, raw: scalar} $token
     */
    public function __construct(array $token)
    {
        $this->type = $token['type'];
        $this->precedence = 0;
        $this->name = $token['value'];
        $this->value = self::extractConstantValue($token);
    }

    /**
     * @param array{type: TokenType::*, value: string, raw: string} $token
     *
     * @return null|scalar
     */
    private static function extractConstantValue(array $token)
    {
        switch ($token['type']) {
            case TokenType::NUMERIC_CONSTANT:
                if (\strpos((string) $token['value'], '.') !== false) {
                    return (double) $token['value'];
                }

                return (int) $token['value'];

            case TokenType::STRING_CONSTANT:
                return (string) $token['value'];

            case TokenType::NULL_CONSTANT:
                return null;

            default:
                throw new SQLFakeRuntimeException("Attempted to assign invalid token type {$token['type']} to Constant Expression");
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

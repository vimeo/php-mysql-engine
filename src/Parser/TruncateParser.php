<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\TruncateQuery;

final class TruncateParser
{
    /**
     * @var int
     */
    private $pointer = 0;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @var string
     */
    private $sql;

    /**
     * @param array<int, Token> $tokens
     * @param string            $sql
     */
    public function __construct(array $tokens, string $sql)
    {
        $this->tokens = $tokens;
        $this->sql = $sql;
    }

    /**
     * @return TruncateQuery
     * @throws ParserException
     */
    public function parse() : TruncateQuery
    {
        $token = $this->tokens[$this->pointer] ?? null;
        if ($token === null) {
            throw new ParserException('Parser error: invalid tokens');
        }
        if ($token->value !== 'TRUNCATE') {
            throw new ParserException('Parser error: expected TRUNCATE');
        }

        $this->pointer++;

        $token = $this->tokens[$this->pointer] ?? null;

        if ($token!==null && $token->value === 'TABLE' && $token->type===TokenType::RESERVED) {
            $this->pointer++;
            $token = $this->tokens[$this->pointer] ?? null;
        }

        if ($token === null || $token->type !== TokenType::IDENTIFIER) {
            throw new ParserException('Expected table name after TRUNCATE');
        }

        return new TruncateQuery($token->value, $this->sql);
    }
}

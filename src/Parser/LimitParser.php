<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;

final class LimitParser
{
    /**
     * @var int
     */
    private $pointer;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(int $pointer, array $tokens)
    {
        $this->pointer = $pointer;
        $this->tokens = $tokens;
    }

    /**
     * @return array{0:int, 1:array{rowcount:int, offset:int}}
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'LIMIT') {
            throw new ParserException("Parser error: expected LIMIT");
        }
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
        if ($next === null || $next->type !== TokenType::NUMERIC_CONSTANT) {
            throw new ParserException("Expected integer after LIMIT");
        }
        $limit = (int) $next->value;
        $offset = 0;
        $next = $this->tokens[$this->pointer + 1] ?? null;
        if ($next !== null) {
            if ($next->value === 'OFFSET') {
                $this->pointer += 2;
                $next = $this->tokens[$this->pointer] ?? null;
                if ($next === null || $next->type !== TokenType::NUMERIC_CONSTANT) {
                    throw new ParserException("Expected integer after OFFSET");
                }
                $offset = (int) $next->value;
            } else {
                if ($next->value === ',') {
                    $this->pointer += 2;
                    $next = $this->tokens[$this->pointer] ?? null;
                    if ($next === null || $next->type !== TokenType::NUMERIC_CONSTANT) {
                        throw new ParserException("Expected integer after OFFSET");
                    }
                    $offset = $limit;
                    $limit = (int) $next->value;
                }
            }
        }
        return [$this->pointer, ['rowcount' => $limit, 'offset' => $offset]];
    }
}

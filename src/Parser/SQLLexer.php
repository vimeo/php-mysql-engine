<?php
namespace MysqlEngine\Parser;

use function preg_split, preg_match;

final class SQLLexer
{
    private const TOKEN_SPLIT_REGEX = '/('
        . '\\<\\=\\>|\\r\\n|\\!\\=|\\>\\=|\\<\\=|\\<\\>'
        . '|\\<\\<|\\>\\>|\\:\\=|&&|\\|\\||\\:\\=|\\/\\*'
        . '|\\*\\/|\\-\\-|\\>|\\<|\\||\\=|\\^|\\(|\\)|\\t'
        . '|\\n|\'|"|`|,|\\s|\\+|\\-|\\*|\\~|\\/|;|\\\\'
        . ')/';

    /**
     * @return list<array{string,int}>
     */
    public function lex(string $sql)
    {
        $tokens = preg_split(
            self::TOKEN_SPLIT_REGEX,
            $sql,
            -1,
            \PREG_SPLIT_DELIM_CAPTURE | \PREG_SPLIT_NO_EMPTY | \PREG_SPLIT_OFFSET_CAPTURE
        );

        if ($tokens === false) {
            throw new LexerException('Error in regular expression');
        }

        if (preg_match('![/#-]!', $sql)) {
            $tokens = $this->groupComments($tokens);
        }
        if (strpos($sql, '\\') !== false) {
            $tokens = $this->groupEscapeSequences($tokens);
        }
        return $this->groupQuotedTokens($tokens);
    }

    /**
     * @param list<array{string,int}> $tokens
     *
     * @return list<array{string,int}>
     */
    private function groupComments(array $tokens)
    {
        $comment = null;
        $inline = false;
        $count = \count($tokens);
        $escape_next = false;
        $quote = null;
        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if ($comment !== null) {
                if ($inline && ($token[0] === "\n" || $token[0] === "\r\n")) {
                    unset($tokens[$comment]);
                    $comment = null;
                } else {
                    unset($tokens[$i]);
                    $tokens[$comment][0] .= $token[0];
                }
                if (!$inline && $token[0] === "*/") {
                    unset($tokens[$comment]);
                    $comment = null;
                }
                continue;
            }
            if (!$escape_next && ($token[0] === '\'' || $token[0] === '"')) {
                if ($quote !== null && $quote[0] === $token[0]) {
                    $quote = null;
                } else {
                    if ($quote !== null) {
                        continue;
                    } else {
                        $quote = $token;
                    }
                }
            }
            if ($token[0] === '\\') {
                $escape_next = true;
            } else {
                $escape_next = false;
            }
            if ($quote !== null) {
                continue;
            }
            if ($token[0] === "--") {
                $comment = $i;
                $inline = true;
            }
            if ($token[0][0] === "#") {
                $comment = $i;
                $inline = true;
            }
            if ($token[0] === "/*") {
                $comment = $i;
                $inline = false;
            }
        }

        return array_values($tokens);
    }

    /**
     * @param list<array{string,int}> $tokens
     *
     * @return list<array{string,int}>
     */
    private function groupEscapeSequences(array $tokens)
    {
        $tokenCount = \count($tokens);
        $i = 0;
        while ($i < $tokenCount) {
            if (\substr($tokens[$i][0], -1) === '\\') {
                $i++;
                if (\array_key_exists($i, $tokens)) {
                    $tokens[$i - 1][0] .= $tokens[$i][0];
                    unset($tokens[$i]);
                }
            }
            $i++;
        }
        return array_values($tokens);
    }

    /**
     * @param list<array{string,int}> $tokens
     *
     * @return list<array{string,int}>
     */
    private function groupQuotedTokens(array $tokens)
    {
        $i = 0;
        $count = \count($tokens);

        while ($i < $count) {
            $token = $tokens[$i];

            if ($token[0] === '\'' || $token[0] === '"' || $token[0] === '`') {
                $quote = $token;
                $quote_start = $i;
                $i++;
                $found_match = false;

                while ($i < $count) {
                    $t = $tokens[$i];
                    $token[0] .= $t[0];
                    unset($tokens[$i]);
                    $i++;

                    if ($t[0] === $quote[0]) {
                        $found_match = true;
                        if ($i < $count && \mb_substr($tokens[$i][0], 0, 1) === '.') {
                            $t = $tokens[$i];
                            $token[0] .= $t[0];
                            unset($tokens[$i]);
                            $i++;
                        }
                        break;
                    }
                }

                if (!$found_match) {
                    throw new LexerException("Unbalanced quote {$quote[0]}");
                }

                $tokens[$quote_start] = $token;
                continue;
            }

            $i++;
        }

        return array_values($tokens);
    }
}

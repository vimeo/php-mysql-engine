<?php
namespace Vimeo\MysqlEngine\Parser;

use function preg_split, preg_match;

final class SQLLexer
{
    private const TOKEN_SPLIT_REGEX = '/('
        . '\\<\\=\\>|\\r\\n|\\!\\=|\\>\\=|\\<\\=|\\<\\>'
        . '|\\<\\<|\\>\\>|\\:\\=|&&|\\|\\||\\:\\=|\\/\\*'
        . '|\\*\\/|\\-\\-|\\>|\\<|\\||\\=|\\^|\\(|\\)|\\t'
        . '|\\n|\'|"|`|,|@|\\s|\\+|\\-|\\*|\\/|;|\\\\'
        . ')/';

    /**
     * @return list<string>
     */
    public function lex(string $sql)
    {
        $tokens = preg_split(
            self::TOKEN_SPLIT_REGEX,
            $sql,
            -1,
            \PREG_SPLIT_DELIM_CAPTURE | \PREG_SPLIT_NO_EMPTY
        );

        if (preg_match('![/#-]!', $sql)) {
            $tokens = $this->groupComments($tokens);
        }
        if (strpos($sql, '\\') !== false) {
            $tokens = $this->groupEscapeSequences($tokens);
        }
        return $this->groupQuotedTokens($tokens);
    }

    /**
     * @param list<string> $tokens
     *
     * @return list<string>
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
                if ($inline && ($token === "\n" || $token === "\r\n")) {
                    unset($tokens[$comment]);
                    $comment = null;
                } else {
                    unset($tokens[$i]);
                    $tokens[$comment] .= $token;
                }
                if (!$inline && $token === "*/") {
                    unset($tokens[$comment]);
                    $comment = null;
                }
                continue;
            }
            if (!$escape_next && ($token === '\'' || $token === '"')) {
                if ($quote !== null && $quote === $token) {
                    $quote = null;
                } else {
                    if ($quote !== null) {
                        continue;
                    } else {
                        $quote = $token;
                    }
                }
            }
            if ($token === '\\') {
                $escape_next = true;
            } else {
                $escape_next = false;
            }
            if ($quote !== null) {
                continue;
            }
            if ($token === "--") {
                $comment = $i;
                $inline = true;
            }
            if ($token[0] === "#") {
                $comment = $i;
                $inline = true;
            }
            if ($token === "/*") {
                $comment = $i;
                $inline = false;
            }
        }

        return array_values($tokens);
    }

    /**
     * @param list<string> $tokens
     *
     * @return list<string>
     */
    private function groupEscapeSequences(array $tokens)
    {
        $tokenCount = \count($tokens);
        $i = 0;
        while ($i < $tokenCount) {
            if (\substr($tokens[$i], -1) === '\\') {
                $i++;
                if (\array_key_exists($i, $tokens)) {
                    $tokens[$i - 1] .= $tokens[$i];
                    unset($tokens[$i]);
                }
            }
            $i++;
        }
        return array_values($tokens);
    }

    /**
     * @param list<string> $tokens
     *
     * @return list<string>
     */
    private function groupQuotedTokens(array $tokens)
    {
        $i = 0;
        $count = \count($tokens);

        while ($i < $count) {
            $token = $tokens[$i];

            if ($token === '\'' || $token === '"' || $token === '`') {
                $quote = $token;
                $quote_start = $i;
                $i++;
                $found_match = false;

                while ($i < $count) {
                    $t = $tokens[$i];
                    $token .= $t;
                    unset($tokens[$i]);
                    $i++;

                    if ($t === $quote) {
                        $found_match = true;
                        if ($i < $count && \mb_substr($tokens[$i], 0, 1) === '.') {
                            $t = $tokens[$i];
                            $token .= $t;
                            unset($tokens[$i]);
                            $i++;
                        }
                        break;
                    }
                }

                if (!$found_match) {
                    throw new SQLFakeParseException("Unbalanced quote {$quote}");
                }

                $tokens[$quote_start] = $token;
                continue;
            }

            $i++;
        }

        return array_values($tokens);
    }
}

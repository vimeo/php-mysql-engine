<?hh // strict

namespace Slack\DBMock;

use function preg_split, preg_match;
use namespace HH\Lib\{C, Str};

/**
 * Split a SQL string into tokens to prepare for the parser
 * This does some very light processing like combining comments and quoted strings
 */
final class SQLLexer {

  // this regex contains all MySQL operands and words patterns (including whitespace) that can separate SQL Tokens
  // the longer ones come first which is important because of how preg_split works
  private static string $token_split_regex =
    '/(\<\=\>|\r\n|\!\=|\>\=|\<\=|\<\>|\<\<|\>\>|\:\=|\\|&&|\|\||\:\=|\/\*|\*\/|\-\-|\>|\<|\||\=|\^|\(|\)|\t|\n|\'|"|`|,|@|\s|\+|\-|\*|\/|;)/';

  public function lex(string $sql): vec<string> {

    $tokens = preg_split(self::$token_split_regex, $sql, null, \PREG_SPLIT_DELIM_CAPTURE | \PREG_SPLIT_NO_EMPTY)
      |> dict($$);

    // first process SQL comments, grouping them into one token so that the rest of the statements don't need to worry about handling comments
    // since comments are rare, save a bit of time by skipping this if the SQL can't possibly contain comments
    if (preg_match('![/#-]!', $sql)) {
      $tokens = $this->groupComments($tokens);
    }

    // for a string like 'Scott\\'s' this puts the ' character in the right place, necessary for balance_quotes to works
    // since backslashes are rare, save time by skipping this if backslashes aren't present
    if (Str\contains($sql, '\\')) {
      $tokens = $this->groupEscapeSequences($tokens);
    }

    // quotes are common, always do this
    return $this->groupQuotedTokens($tokens);
  }

  // group comments into a single token, then remove them
  private function groupComments(dict<int, string> $tokens): dict<int, string> {

    $comment = null;
    $inline = false;
    $count = C\count($tokens);
    $escape_next = false;
    $quote = null;

    for ($i = 0; $i < $count; $i++) {

      $token = $tokens[$i];

      // are we inside a comment already?
      if ($comment !== null) {
        if ($inline && ($token === "\n" || $token === "\r\n")) {
          unset($tokens[$comment]);
          $comment = null;
        } else {
          unset($tokens[$i]);
          $tokens[$comment] .= $token;
        }

        if (!$inline && ($token === "*/")) {
          unset($tokens[$comment]);
          $comment = null;
        }
        continue;
      }

      // we need to handle sequences that look like comments, but are inside quoted strings. to do that, we also need to know when quoted strings start and end
      // since a comment could contain a quote, and a quote could contain a comment, we can't safely process either first without being aware of the other
      // so first we check if the next token should be escaped, and then if it's a quote character
      if ($token === '\\') {
        $escape_next = true;
      } else {
        $escape_next = false;
      }

      if (!$escape_next && C\contains_key(keyset['\'', '"'], $token)) {
        if ($quote !== null && $quote === $token) {
          $quote = null;
        } else {
          $quote = $token;
        }
      }

      // if we are inside a quoted string, do not check for comment sequences
      if ($quote !== null) {
        continue;
      }

      // MySQL requires a space after double dash for it to be counted as a comment: https://dev.mysql.com/doc/refman/5.7/en/ansi-diff-comments.html
      if ($token === "--") {
        $comment = $i;
        $inline = true;
      }

      // hash comments don't require a space
      if (Str\starts_with($token, "#")) {
        $comment = $i;
        $inline = true;
      }

      if ($token === "/*") {
        $comment = $i;
        $inline = false;
      }
    }

    // re-key
    return dict(vec($tokens));
  }

  private function groupEscapeSequences(dict<int, string> $tokens): dict<int, string> {
    $tokenCount = C\count($tokens);
    $i = 0;

    while ($i < $tokenCount) {

      if (Str\ends_with($tokens[$i], '\\')) {
        $i++;
        if (C\contains_key($tokens, $i)) {
          $tokens[$i - 1] .= $tokens[$i];
          unset($tokens[$i]);
        }
      }
      $i++;
    }

    // re-key
    return dict(vec($tokens));
  }

  private function groupQuotedTokens(dict<int, string> $tokens): vec<string> {
    $i = 0;
    $count = C\count($tokens);
    while ($i < $count) {

      $token = $tokens[$i];

      // single quotes, double quotes, or backticks
      // when we find a quote, seek forward to the next matching quote and combine all tokens within
      if (C\contains_key(keyset['\'', '"', '`'], $token)) {
        $quote = $token;
        $quote_start = $i;
        $i++;
        $found_match = false;
        while ($i < $count) {
          $t = $tokens[$i];
          // up to and including when we find a match, unroll each new token and add it to the current $token
          $token .= $t;
          unset($tokens[$i]);
          $i++;
          if ($t === $quote) {
            $found_match = true;
            // if the quotes are followed by a dot, add it in too (e.g. `database`.tablename)
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
          throw new DBMockParseException("Unbalanced quote $quote");
        }

        $tokens[$quote_start] = $token;
        continue;
      }

      $i++;
    }

    return vec($tokens);
  }
}

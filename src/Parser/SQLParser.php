<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Regex, Str, Vec};

final class SQLParser {

  // static is better for memoize. Memoize so that the same SQL query passed in a second time is parsed from cache
  <<__Memoize>>
  public static function parse(string $sql): Query {
    $tokens = (new SQLLexer())->lex($sql);
    $tokens = self::buildTokenListFromLexemes($tokens);

    $token = $tokens[0];
    // handle a query like (SELECT 1), just strip the surrounding parens
    if ($token['type'] === TokenType::PAREN) {
      $close = self::findMatchingParen(0, $tokens);
      $tokens = Vec\slice($tokens, 1, $close - 1);
      $token = $tokens[0];
    }

    if ($token['type'] !== TokenType::CLAUSE) {
      throw new DBMockParseException("Unexpected {$token['value']}");
    }

    switch ($token['value']) {
      case 'SELECT':
        $select = new SelectParser(0, $tokens, $sql);
        list($pointer, $query) = $select->parse();
        // we still have something left here after parsing the whole top level query? hopefully it's a multi-query keyword
        if (C\contains_key($tokens, $pointer)) {
          $next = $tokens[$pointer] ?? null;
          $val = $next ? $next['value'] : 'null';
          while ($next !== null && C\contains_key(keyset['UNION', 'INTERSECT', 'EXCEPT'], $next['value'])) {
            $type = $next['value'];
            if ($next['value'] === 'UNION') {
              $next_plus = $tokens[$pointer + 1];
              if ($next_plus['value'] === 'ALL') {
                $type = 'UNION_ALL';
                $pointer++;
              }
            }
            $pointer++;
            $select = new SelectParser($pointer, $tokens, $sql);
            list($pointer, $q) = $select->parse();
            $query->addMultiQuery(MultiOperand::assert($type), $q);

            $next = $tokens[$pointer] ?? null;
          }
        }
        return $query;
      case 'UPDATE':
        $update = new UpdateParser($tokens, $sql);
        return $update->parse();
      case 'DELETE':
        $delete = new DeleteParser($tokens, $sql);
        return $delete->parse();
      case 'INSERT':
        $insert = new InsertParser($tokens, $sql);
        return $insert->parse();
      default:
        throw new DBMockParseException("Unexpected {$token['value']}");
    }

    throw new DBMockParseException("Parse error: unexpected end of input");
  }

  /*
   * Lexemes are just a vec of strings from the lexer
   * This builds them into typed token shapes based on lists of reserved keywords and surrounding context
   */
  private static function buildTokenListFromLexemes(vec<string> $tokens): token_list {
    $out = vec[];
    $count = C\count($tokens);
    foreach ($tokens as $i => $token) {

      // skip white space, but tack it onto the tokens in another field for when we need to assemble the expression list
      if (Str\trim($token) === '') {
        $k = C\last_key($out);
        if ($k !== null) {
          $previous = $out[$k];
          $previous['raw'] .= $token;
          $out[$k] = $previous;
        }
        continue;
      }

      if (Str\to_int($token) is nonnull) {
        $out[] = shape(
          'type' => TokenType::NUMERIC_CONSTANT,
          'value' => $token,
          'raw' => $token,
        );
        continue;
      } elseif (C\contains_key(keyset['\'', '"'], $token[0])) {
        // chop off the quotes before storing the value
        $raw = $token;
        $token = Str\slice($token, 1, Str\length($token) - 2);
        // unescape everything except for % and _ (which only get unescaped during LIKE operations)
        // there are a few other special sequnces we leave unescaped like \r, \n, \t, \b, \Z, \0
        // https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
        $token = Regex\replace($token, re"/\\\\([^%_rntbZ0])/", '\1');
        $out[] = shape(
          'type' => TokenType::STRING_CONSTANT,
          'value' => $token,
          'raw' => $raw,
        );
        continue;
      } elseif ($token[0] === '`') {
        $raw = $token;
        // Only chop off the ` if it's fully wrapping the identifier
        if (Str\ends_with($token, '`')) {
          $token = Str\strip_prefix($token, '`') |> Str\strip_suffix($$, '`');
        } else if (Str\ends_with($token, '`.')) {
          // for `foo`., it becomes foo.
          $token = Str\strip_prefix($token, '`') |> Str\strip_suffix($$, '`.') |> $$.'.';
        }

        // if we find an identifier and previous token ended with ., smush them together
        $previous_key = C\last_key($out);
        if (
          $previous_key is nonnull &&
          $out[$previous_key]['type'] === TokenType::IDENTIFIER &&
          Str\ends_with($out[$previous_key]['value'], '.')
        ) {
          $out[$previous_key]['value'] .= $token;
          continue;
        }

        $out[] = shape(
          'type' => TokenType::IDENTIFIER,
          'value' => $token,
          'raw' => $raw,
        );
        continue;
      } elseif ($token[0] === '(') {
        $out[] = shape('type' => TokenType::PAREN, 'value' => $token, 'raw' => $token);
        continue;
      } elseif ($token === '*') {
        // the * character is special because it's sometimes an operator and most of the time it means "all columns"
        $k = C\last_key($out);
        if ($k === null) {
          throw new DBMockParseException("Parse error: unexpected *");
        }
        $previous = $out[$k];
        $out[$k] = $previous;
        if (
          !C\contains_key(
            keyset[
              TokenType::NUMERIC_CONSTANT,
              TokenType::STRING_CONSTANT,
              TokenType::NULL_CONSTANT,
              TokenType::IDENTIFIER,
            ],
            $previous['type'],
          ) &&
          $previous['value'] !== ')'
        ) {
          $out[] = shape(
            'type' => TokenType::IDENTIFIER,
            'value' => $token,
            'raw' => $token,
          );
          continue;
        } elseif ($previous['type'] === TokenType::IDENTIFIER && Str\ends_with($previous['value'], '.')) {
          // previous ended like "foo.", we should keep "foo.*" together as one token
          $previous['value'] .= $token;
          $previous['raw'] .= $token;
          $out[$k] = $previous;
          continue;
        }
      } elseif ($token === '-' || $token === '+') {
        // these operands can be binary or unary operands
        // for example "SELECT -5" or "SELECT 7 - 5" are both valid, in the first case it's a unary op
        // we don't just combine it into the constant, because it's also valid for columns like SELECT -some_column FROM table
        // similar to * we can identify this now based on context
        $k = C\last_key($out);
        if ($k === null) {
          throw new DBMockParseException("Parse error: unexpected {$token}");
        }
        $previous = $out[$k];
        if (
          !C\contains_key(
            keyset[
              TokenType::NUMERIC_CONSTANT,
              TokenType::STRING_CONSTANT,
              TokenType::NULL_CONSTANT,
              TokenType::IDENTIFIER,
            ],
            $previous['type'],

          ) &&
          $previous['value'] !== ')'
        ) {
          if ($token === '-') {
            $op = 'UNARY_MINUS';
          } else {
            $op = 'UNARY_PLUS';
          }
          $out[] = shape(
            'type' => TokenType::OPERATOR,
            'value' => $op,
            'raw' => $token,
          );
          continue;
        }
      }

      $token_upper = Str\uppercase($token);

      if ($token_upper === 'NULL') {
        $out[] = shape(
          'type' => TokenType::NULL_CONSTANT,
          'value' => $token,
          'raw' => $token,
        );
      } elseif ($token_upper === 'TRUE') {
        $out[] = shape(
          'type' => TokenType::NUMERIC_CONSTANT,
          'value' => '1',
          'raw' => $token,
        );
      } elseif ($token_upper === 'FALSE') {
        $out[] = shape(
          'type' => TokenType::NUMERIC_CONSTANT,
          'value' => '0',
          'raw' => $token,
        );
      } elseif (C\contains_key(self::CLAUSES, $token_upper)) {
        $out[] = shape(
          'type' => TokenType::CLAUSE,
          'value' => $token_upper,
          'raw' => $token,
        );
      } elseif (
        C\contains_key(self::OPERATORS, $token_upper) &&
        !self::isFunctionVersionOfOperator($token_upper, $i, $count, $tokens)
      ) {
        $out[] = shape(
          'type' => TokenType::OPERATOR,
          'value' => $token_upper,
          'raw' => $token,
        );
      } elseif (C\contains_key(self::RESERVED_WORDS, $token_upper)) {
        $out[] = shape(
          'type' => TokenType::RESERVED,
          'value' => $token_upper,
          'raw' => $token,
        );
      } elseif (C\contains_key(self::SEPARATORS, $token_upper)) {
        $out[] = shape(
          'type' => TokenType::SEPARATOR,
          'value' => $token_upper,
          'raw' => $token,
        );
      } elseif ($i < $count - 1 && $tokens[$i + 1] === '(') {
        $out[] = shape(
          'type' => TokenType::SQLFUNCTION,
          'value' => $token_upper,
          'raw' => $token,
        );
      } else {
        // if we find an identifier and previous token ended with ., smush them together
        $previous_key = C\last_key($out);
        if (
          $previous_key is nonnull &&
          $out[$previous_key]['type'] === TokenType::IDENTIFIER &&
          Str\ends_with($out[$previous_key]['value'], '.')
        ) {
          $out[$previous_key]['value'] .= $token;
          continue;
        }
        $out[] = shape(
          'type' => TokenType::IDENTIFIER,
          'value' => $token,
          'raw' => $token,
        );
      }
    }
    return $out;
  }

  /**
   * There seems to be a few operators that also exists as functions. MOD() for example.
   * So we check if this particular find is an operator or a function.
   */
  private static function isFunctionVersionOfOperator(
    string $token_upper,
    int $i,
    int $count,
    vec<string> $tokens,
  ): bool {
    return $token_upper === 'MOD' && $i < $count - 1 && $tokens[$i + 1] === '(';
  }

  public static function findMatchingParen(int $pointer, token_list $tokens): int {
    $paren_count = 0;
    $remaining_tokens = Vec\drop($tokens, $pointer);
    $token_count = C\count($remaining_tokens);
    foreach ($remaining_tokens as $i => $token) {
      if ($token['type'] === TokenType::PAREN) {
        $paren_count++;
      } elseif ($token['type'] === TokenType::SEPARATOR && $token['value'] === ')') {
        $paren_count--;
        if ($paren_count === 0) {
          return $pointer + $i;
        }
      }
    }

    // if we get here, we didn't find the close
    throw new DBMockParseException("Unclosed parentheses at index $pointer");
  }

  /*
  * Skip over index hints, but might as well still syntax validate them while doing so
  * Examples of index hints:
  * FROM table1 USE INDEX (col1_index,col2_index)
  * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2)
  */
  public static function skipIndexHints(int $pointer, token_list $tokens): int {
    $next_pointer = $pointer + 1;
    $next = $tokens[$next_pointer] ?? null;
    while (
      $next !== null &&
      $next['type'] === TokenType::RESERVED &&
      C\contains_key(keyset['USE', 'IGNORE', 'FORCE'], $next['value'])
    ) {
      $pointer += 2;
      $hint_type = $next['value'];
      $next = $tokens[$pointer] ?? null;
      if ($next === null || !C\contains_key(keyset['INDEX', 'KEY'], $next['value'])) {
        throw new DBMockParseException("Expected INDEX or KEY in index hint");
      }

      $pointer++;
      $next = $tokens[$pointer] ?? null;
      if ($next === null) {
        // USE hint is allowed to stop at "USE INDEX" which means "use no indexes"
        if ($hint_type === 'USE') {
          $pointer--;
          return $pointer;
        }
        throw new DBMockParseException("Expected expected FOR or index list in index hint");
      }

      if ($next['value'] === 'FOR') {
        $pointer++;
        $next = $tokens[$pointer] ?? null;
        if ($next === null) {
          throw new DBMockParseException("Expected JOIN, ORDER BY, or GROUP BY after FOR in index hint");
        } elseif ($next['value'] === 'JOIN') {
          //this is fine
          $pointer++;
          $next = $tokens[$pointer] ?? null;
        } elseif (C\contains_key(keyset['GROUP', 'ORDER'], $next['value'])) {
          $pointer++;
          $next = $tokens[$pointer] ?? null;
          if ($next === null || $next['value'] !== 'BY') {
            throw new DBMockParseException("Expected BY in index hint after GROUP or ORDER");
          }

          $pointer++;
          $next = $tokens[$pointer] ?? null;
        } else {
          throw new DBMockParseException("Expected JOIN, ORDER BY, or GROUP BY after FOR in index hint");
        }
      }

      if ($next === null || $next['type'] !== TokenType::PAREN) {
        // USE hint is allowed to stop at "USE INDEX" which means "use no indexes"
        if ($hint_type === 'USE') {
          $pointer--;
          return $pointer;
        }
        throw new DBMockParseException("Expected index expression after index hint");
      }

      $closing_paren_pointer = SQLParser::findMatchingParen($pointer, $tokens);
      $arg_tokens = Vec\slice($tokens, $pointer + 1, $closing_paren_pointer - $pointer - 1);
      if (!C\count($arg_tokens)) {
        throw new DBMockParseException("Expected at least one argument to index hint");
      }
      $count = 0;
      foreach ($arg_tokens as $arg) {
        $count++;
        if ($count % 2 === 1) {
          if ($arg['type'] !== TokenType::IDENTIFIER) {
            throw new DBMockParseException("Expected identifier in index hint");
          }
        } elseif ($arg['value'] !== ',') {
          throw new DBMockParseException("Expected , or ) after index hint");
        }
      }

      $pointer = $closing_paren_pointer;

      // you can have multiple index hints... so if the next token starts another index hint we can go back into this while loop
      $next_pointer = $pointer + 1;
      $next = $tokens[$next_pointer] ?? null;

    }

    return $pointer;
  }

  /*
  * Skip over lock hints, but might as well still syntax validate them while doing so
  * Examples of index hints:
  * FROM table1 WHERE ... FOR UPDATE
  * FROM table1 WHERE ... LOCK IN SHARE MODE
  */
  public static function skipLockHints(int $pointer, token_list $tokens): int {
    $next_pointer = $pointer + 1;
    $next = $tokens[$next_pointer] ?? null;

    if ($next !== null && $next['type'] === TokenType::RESERVED) {
      if ($next['value'] === 'FOR') {
        // skip over FOR UDPATE while validating it
        $next_pointer++;
        $next = $tokens[$next_pointer] ?? null;
        if ($next === null) {
          throw new DBMockParseException("Expected keyword after FOR");
        }
        // skip over FOR UPDATE
        if ($next['value'] === 'UPDATE') {
          return $pointer + 2;
        }

        throw new DBMockParseException("Unexpected keyword {$next['value']} after FOR");
      } elseif ($next['value'] === 'LOCK') {
        // skip over LOCK IN SHARE MODE while validating it
        $expected = vec['IN', 'SHARE', 'MODE'];
        foreach ($expected as $index => $keyword) {
          $next = $tokens[$next_pointer + $index + 1] ?? null;
          if ($next === null || $next['value'] !== $keyword) {
            throw new DBMockParseException("Unexpected keyword near LOCK");
          }
        }
        return $pointer + 4;
      }
    }

    return $pointer;
  }

  const keyset<string> CLAUSES = keyset[
    'SELECT',
    'FROM',
    'WHERE',
    'GROUP',
    'HAVING',
    'LIMIT',
    'ORDER',
    'UPDATE',
    'SET',
    'DELETE',
    'UNION',
    'EXCEPT',
    'INTERSECT',
    'INSERT',
    'VALUES',
  ];

  // left PAREN is not in here because it triggers special logic, so it has its own type of PAREN
  const keyset<string> SEPARATORS = keyset[
    ')',
    ',',
    ';',
  ];

  const keyset<string> OPERATORS = keyset[
    'INTERVAL',
    'COLLATE',
    '!',
    '~',
    '^',
    '*',
    '/',
    'DIV',
    '%',
    'MOD',
    '-',
    '+',
    '<<',
    '>>',
    '&',
    '|',
    '=',
    '<=>',
    '>=',
    '>',
    '<=',
    '<',
    '<>',
    '!=',
    'IS',
    'LIKE',
    'REGEXP',
    'IN',
    'EXISTS',
    'BETWEEN',
    'CASE',
    'WHEN',
    'THEN',
    'ELSE',
    'END',
    'NOT',
    'AND',
    '&&',
    'XOR',
    'OR',
    '||',
  ];

  const keyset<string> RESERVED_WORDS = keyset[
    'ASC',
    'DESC',
    'AS',
    'WITH',
    'ON',
    'OFFSET',
    'BY',
    'INTO',
    'ALL',
    'DISTINCT',
    'DISTINCTROW',
    'SQL_CALC_FOUND_ROWS',
    'HIGH_PRIORITY',
    'SQL_SMALL_RESULT',
    'SQL_BIG_RESULT',
    'SQL_BUFFER_RESULT',
    'SQL_CACHE',
    'SQL_NO_CACHE',
    'JOIN',
    'INNER',
    'OUTER',
    'LEFT',
    'RIGHT',
    'STRAIGHT_JOIN',
    'NATURAL',
    'USING',
    'CROSS',
    'USE',
    'IGNORE',
    'FORCE',
    'PARTITION',
    'ROLLUP',
    'INDEX',
    'KEY',
    'FOR',
    'LOCK',
    'DUPLICATE',
    'DELAYED',
    'LOW_PRIORITY',
    'HIGH_PRIORITY',
  ];
}

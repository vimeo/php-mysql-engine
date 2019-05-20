<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Vec};

/**
 * Support INSERT statements
 *
 * Supported: INSERT INTO ... and INSERT ... ON DUPLICATE KEY UPDATE
 * Currently not supported: PARTITION inserts and INSERT ... SELECT
 */
final class InsertParser {

  const dict<string, int> CLAUSE_ORDER = dict[
    'INSERT' => 1,
    'COLUMN_LIST' => 2,
    'VALUES' => 3,
    'ON' => 4,
    'SET' => 5,
  ];

  private string $currentClause = 'INSERT';
  private int $pointer = 0;

  public function __construct(private token_list $tokens, private string $sql) {}

  public function parse(): InsertQuery {

    // if we got here, the first token had better be a INSERT
    if ($this->tokens[$this->pointer]['value'] !== 'INSERT') {
      throw new SQLFakeParseException("Parser error: expected INSERT");
    }
    $this->pointer++;

    // ignore these keywords which can come after INSERT
    if (C\contains_key(keyset['LOW_PRIORITY', 'DELAYED', 'HIGH_PRIORITY'], $this->tokens[$this->pointer]['value'])) {
      $this->pointer++;
    }

    // IGNORE can come next and indicates duplicate keys should be ignored
    $ignore_dupes = false;
    if ($this->tokens[$this->pointer]['value'] === 'IGNORE') {
      $ignore_dupes = true;
      $this->pointer++;
    }

    // INTO is optional and has no effect. skip it if present
    if ($this->tokens[$this->pointer]['value'] === 'INTO') {
      $this->pointer++;
    }

    // next token has to be a table name
    $token = $this->tokens[$this->pointer];
    if ($token === null || $token['type'] !== TokenType::IDENTIFIER) {
      throw new SQLFakeParseException("Expected table name after INSERT");
    }
    $this->pointer++;

    $query = new InsertQuery($token['value'], $this->sql, $ignore_dupes);

    $count = C\count($this->tokens);

    $needs_comma = false;
    $end_of_set = false;
    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      // handle VALUES() as a function, like "on duplicate key update column=VALUES(column)"
      if ($this->currentClause === 'SET' && $token['value'] === 'VALUES') {
        $token['type'] = TokenType::SQLFUNCTION;
      }

      switch ($token['type']) {
        case TokenType::CLAUSE:
          // make sure clauses are in order
          if (
            C\contains_key(self::CLAUSE_ORDER, $token['value']) &&
            self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token['value']]
          ) {
            throw new SQLFakeParseException("Unexpected clause {$token['value']}");
          }

          switch ($token['value']) {
            case 'VALUES':
              do {
                $this->pointer++;
                $token = $this->tokens[$this->pointer];
                // VALUES must be followed by paren and then a list of values
                if ($token === null || $token['value'] !== '(') {
                  throw new SQLFakeParseException("Expected ( after VALUES");
                }
                $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);
                $values_tokens = Vec\slice($this->tokens, $this->pointer + 1, $close - $this->pointer - 1);
                $values = $this->parseValues($values_tokens);
                if (C\count($values) !== C\count($query->insertColumns)) {
                  throw new SQLFakeParseException(
                    "Insert list contains ".
                    C\count($query->insertColumns).
                    ' fields, but values clause contains '.
                    C\count($values),
                  );
                }
                $query->values[] = $values;
                $this->pointer = $close;
              } while (($this->tokens[$this->pointer + 1]['value'] ?? null) === ',' && $this->pointer++);
              break;
            default:
              throw new SQLFakeParseException("Unexpected clause {$token['value']}");
          }
          $this->currentClause = $token['value'];
          break;
        case TokenType::IDENTIFIER:
          if ($needs_comma) {
            throw new SQLFakeParseException("Expected , between expressions in INSERT");
          }

          if ($this->currentClause !== 'COLUMN_LIST') {
            throw new SQLFakeParseException("Unexpected token {$token['value']} in INSERT");
          }

          $query->insertColumns[] = $token['value'];
          $needs_comma = true;
          break;
        case TokenType::PAREN:
          // are we opening the insert list?
          if ($this->currentClause === 'INSERT' && $token['value'] === '(') {
            $this->currentClause = 'COLUMN_LIST';
            break;
          }

          throw new SQLFakeParseException("Unexpected (");
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            if (!$needs_comma) {
              throw new SQLFakeParseException("Unexpected ,");
            }
            $needs_comma = false;
          } else if ($this->currentClause === 'COLUMN_LIST' && $needs_comma && $token['value'] === ')') {
            // closing the insert column list?
            $needs_comma = false;
            if (($this->tokens[$this->pointer + 1]['value'] ?? null) !== 'VALUES') {
              throw new SQLFakeParseException("Expected VALUES after insert column list");
            }
            break;
          } else {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::RESERVED:
          if ($token['value'] === 'ON') {
            $expected = vec['DUPLICATE', 'KEY', 'UPDATE'];
            $next_pointer = $this->pointer + 1;
            foreach ($expected as $index => $keyword) {
              $next = $this->tokens[$next_pointer + $index] ?? null;
              if ($next === null || $next['value'] !== $keyword) {
                throw new SQLFakeParseException("Unexpected keyword near ON");
              }
            }
            $this->pointer += 3;
            $p = new SetParser($this->pointer, $this->tokens);
            list($this->pointer, $query->updateExpressions) = $p->parse(/* $skip_set */ true);
            break;
          }
          throw new SQLFakeParseException("Unexpected {$token['value']}");
        default:
          throw new SQLFakeParseException("Unexpected token {$token['value']}");
      }

      $this->pointer++;
    }

    if (C\is_empty($query->insertColumns) || C\is_empty($query->values)) {
      throw new SQLFakeParseException("Missing values to insert");
    }

    return $query;
  }

  /**
   * Parse a VALUES clause into a list of expressions
   */
  protected function parseValues(vec<token> $tokens): vec<Expression> {
    $pointer = 0;
    $count = C\count($tokens);
    $expressions = vec[];

    $needs_comma = false;
    $end_of_set = false;
    while ($pointer < $count) {
      $token = $tokens[$pointer];
      switch ($token['type']) {
        case TokenType::IDENTIFIER:
        case TokenType::NUMERIC_CONSTANT:
        case TokenType::STRING_CONSTANT:
        case TokenType::NULL_CONSTANT:
        case TokenType::OPERATOR:
        case TokenType::SQLFUNCTION:
        case TokenType::PAREN:
          if ($needs_comma) {
            throw new SQLFakeParseException("Expected , between expressions in SET clause near {$token['value']}");
          }
          $expression_parser = new ExpressionParser($tokens, $pointer - 1);
          $start = $pointer;
          list($pointer, $expression) = $expression_parser->buildWithPointer();
          $expressions[] = $expression;
          $needs_comma = true;
          break;
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            if (!$needs_comma) {
              echo "le comma one";
              throw new SQLFakeParseException("Unexpected ,");
            }
            $needs_comma = false;
          } else {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          break;
        default:
          throw new SQLFakeParseException("Unexpected token {$token['value']}");
      }
      $pointer++;
    }
    return $expressions;
  }
}

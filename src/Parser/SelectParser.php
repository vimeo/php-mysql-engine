<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str, Vec};

final class SelectParser {

  const dict<string, int> CLAUSE_ORDER = dict[
    'SELECT' => 1,
    'FROM' => 2,
    'WHERE' => 3,
    'GROUP' => 4,
    'HAVING' => 5,
    'ORDER' => 6,
    'LIMIT' => 7,
  ];

  private string $currentClause = 'SELECT';
  public function __construct(private int $pointer, private token_list $tokens, private string $sql) {}


  public function parse(): (int, SelectQuery) {
    // if we got here, the first token had better be a SELECT
    if ($this->tokens[$this->pointer]['value'] !== 'SELECT') {
      throw new SQLFakeParseException("Parser error: expected SELECT");
    }

    $query = new SelectQuery($this->sql);
    $this->pointer++;
    $count = C\count($this->tokens);

    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      switch ($token['type']) {
        case TokenType::NUMERIC_CONSTANT:
        case TokenType::NULL_CONSTANT:
        case TokenType::STRING_CONSTANT:
        case TokenType::OPERATOR:
        case TokenType::SQLFUNCTION:
        case TokenType::IDENTIFIER:
        case TokenType::PAREN:
          // we should only see these things when we're in the SELECT clause
          // all other clauses should parse their own tokens
          // also check that there has been a delimiter since the last expression if we're adding a new one now
          if ($this->currentClause !== 'SELECT') {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          if ($query->needsSeparator) {
            // we just had an expression and no comma yet. if this is a string or identifier, it must be an alias like "SELECT 1 foo"
            if (
              C\contains_key(keyset[TokenType::IDENTIFIER, TokenType::STRING_CONSTANT], $token['type']) &&
              !$query->mostRecentHasAlias
            ) {
              $query->aliasRecentExpression($token['value']);
              break;
            } else {
              // if the new token isn't an identifier, or the most recent expression had an alias, this is bogus
              throw new SQLFakeParseException("Expected comma between expressions near {$token['value']}");
            }
          }
          $expression_parser = new ExpressionParser($this->tokens, $this->pointer - 1);
          $start = $this->pointer;
          list($this->pointer, $expression) = $expression_parser->buildWithPointer();

          // we should "name" the column based on the entire expression (it may get overwritten by an alias)
          // this is actually important, because otherwise if two expressions exist in the select they might not both be in the results if we don't give them unique names
          // since the result rows are keyed by strings. only do this for non-scalar expressions
          if (!$expression is ColumnExpression && !$expression is ConstantExpression) {
            $name = '';
            $slice = Vec\slice($this->tokens, $start, $this->pointer - $start + 1);
            foreach ($slice as $t) {
              $name .= $t['raw'];
            }
            $expression->name = Str\trim($name);
          }

          $query->addSelectExpression($expression);
          break;
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            if (!$query->needsSeparator) {
              throw new SQLFakeParseException("Unexpected ,");
            }
            $query->needsSeparator = false;
          } elseif ($token['value'] === ';') {
            // this should be the final token. if it's not, throw. otherwise, return
            if ($this->pointer !== $count - 1) {
              throw new SQLFakeParseException("Unexpected tokens after semicolon");
            }
            return tuple($this->pointer, $query);
          } else {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::CLAUSE:
          // make sure clauses are in order
          if (
            C\contains_key(self::CLAUSE_ORDER, $token['value']) &&
            self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token['value']]
          ) {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          $this->currentClause = $token['value'];
          switch ($token['value']) {
            case 'FROM':
              $from = new FromParser($this->pointer, $this->tokens);
              list($this->pointer, $fromClause) = $from->parse();
              $query->fromClause = $fromClause;
              break;
            case 'WHERE':
              $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
              list($this->pointer, $expression) = $expression_parser->buildWithPointer();
              $query->whereClause = $expression;
              break;
            case 'GROUP':
              $this->pointer++;
              $next = $this->tokens[$this->pointer] ?? null;
              $expressions = vec[];
              $sort_directions = vec[];
              if ($next === null || $next['value'] !== 'BY') {
                throw new SQLFakeParseException("Expected BY after GROUP");
              }

              while (true) {
                $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
                $expression_parser->setSelectExpressions($query->selectExpressions);
                list($this->pointer, $expression) = $expression_parser->buildWithPointer();

                // group by and order by support POSITIONAL operators such as "GROUP BY 1". And constants aren't supported.
                // so if a constant comes back in the list, it has to be a positional operator
                // and the columns are 1-indexed in MySQL terms so we subtract one from the $position arg to find the right expression
                if ($expression is ConstantExpression) {
                  // SELECT is evaluated after GROUP BY, so the PositionExpression can only be used for ORDER BY
                  // for GROUP BY it just grabs the associated SELECT expression
                  $position = (int)($expression->value) - 1;

                  $expression = $query->selectExpressions[$position] ?? null;
                  if ($expression === null) {
                    throw new SQLFakeParseException("Invalid positional reference $position in GROUP BY");
                  }
                }

                $expressions[] = $expression;
                $next = $this->tokens[$this->pointer + 1] ?? null;
                // skip over commas and continue the processing, but if it's any other token break out of the loop
                if ($next === null || $next['value'] !== ',') {
                  break;
                }
                $this->pointer++;
              }

              $query->groupBy = $expressions;
              break;
            case 'ORDER':
              $p = new OrderByParser($this->pointer, $this->tokens, $query->selectExpressions);
              list($this->pointer, $query->orderBy) = $p->parse();
              break;
            case 'HAVING':
              // same as where, except we add select expressions here
              $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
              $expression_parser->setSelectExpressions($query->selectExpressions);
              list($this->pointer, $expression) = $expression_parser->buildWithPointer();
              $query->havingClause = $expression;
              break;
            case 'LIMIT':
              $p = new LimitParser($this->pointer, $this->tokens);
              list($this->pointer, $query->limitClause) = $p->parse();
              break;
            case 'UNION':
            case 'EXCEPT':
            case 'INTERSECT':
              // return control back to parent, so that if we are at top level we can add this and otherwise not
              return tuple($this->pointer, $query);
              break;
            default:
              throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          // after adding a clause like FROM or WHERE, skip over any locking hints
          $this->pointer = SQLParser::skipLockHints($this->pointer, $this->tokens);
          break;
        case TokenType::RESERVED:
          switch ($token['value']) {
            case 'AS':
              // seek forward for identifier, then add alias to most recent expression
              $this->pointer++;
              $next = $this->tokens[$this->pointer] ?? null;
              if (
                $next === null ||
                !C\contains_key(keyset[TokenType::IDENTIFIER, TokenType::STRING_CONSTANT], $next['type'])
              ) {
                throw new SQLFakeParseException("Expected alias name after AS");
              }
              $query->aliasRecentExpression($next['value']);
              break;
            case 'DISTINCT':
            case 'DISTINCTROW':
            case 'ALL':
            case 'HIGH_PRIORITY':
            case 'SQL_CALC_FOUND_ROWS':
            case 'HIGH_PRIORITY':
            case 'SQL_SMALL_RESULT':
            case 'SQL_BIG_RESULT':
            case 'SQL_BUFFER_RESULT':
            case 'SQL_CACHE':
            case 'SQL_NO_CACHE':
              // DISTINCTROW is an alias for DISTINCT
              if ($token['value'] === 'DISTINCTROW') {
                $token['value'] = 'DISTINCT';
              }
              $query->addOption($token['value']);
              break;
            default:
              throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          break;
      }

      $this->pointer++;
    }

    // check if query is well formed here? well basically it just has to have at least one expression in the SELECT clause
    return tuple($this->pointer, $query);
  }
}

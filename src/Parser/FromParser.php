<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Str, Vec};

final class FromParser {

  public function __construct(private int $pointer, private token_list $tokens) {}

  public function parse(): (int, FromClause) {

    // if we got here, the first token had better be a SELECT
    if ($this->tokens[$this->pointer]['value'] !== 'FROM') {
      throw new DBMockParseException("Parser error: expected FROM");
    }
    $from = new FromClause();
    $this->pointer++;
    $count = C\count($this->tokens);

    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      switch ($token['type']) {
        case TokenType::STRING_CONSTANT:
          if (!$from->mostRecentHasAlias) {
            $from->aliasRecentExpression((string)$token['value']);
            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
          } else {
            throw new DBMockParseException("Unexpected string constant {$token['raw']}");
          }
          break;
        case TokenType::IDENTIFIER:
          if (C\count($from->tables) === 0) {
            $table = shape(
              'name' => $token['value'],
              'join_type' => JoinType::JOIN,
            );
            $from->addTable($table);
            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
          } elseif (!$from->mostRecentHasAlias) {
            $from->aliasRecentExpression((string)$token['value']);
            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
          }
          break;
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            $this->pointer++;
            $next = $this->tokens[$this->pointer] ?? null;
            if ($next === null) {
              throw new DBMockParseException("Expected token after ,");
            }
            $table = $this->getTableOrSubquery($next);
            $table['join_type'] = JoinType::CROSS;
            $from->addTable($table);
            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
          } else {
            throw new DBMockParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::CLAUSE:
          // we made it to the next clause, time to return
          // let parent process this keyword
          return tuple($this->pointer - 1, $from);
        case TokenType::RESERVED:
          switch ($token['value']) {
            case 'AS':
              // seek forward for identifier, then add alias to most recent expression
              $this->pointer++;
              $next = $this->tokens[$this->pointer] ?? null;
              if ($next === null || $next['type'] !== TokenType::IDENTIFIER) {
                throw new DBMockParseException("Expected identifer after AS");
              }
              $from->aliasRecentExpression($next['value']);
              $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
              break;
            case 'JOIN':
            case 'INNER':
            case 'LEFT':
            case 'RIGHT':
            case 'STRAIGHT_JOIN':
            case 'NATURAL':
            case 'CROSS':
              $last = C\last($from->tables);
              if ($last === null) {
                throw new DBMockParseException("Parser error: unexpected join keyword");
              }
              $join = $this->buildJoin($last['name'], $token);
              $from->addTable($join);
              break;
            default:
              throw new DBMockParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::PAREN:
          // this must be a subquery in the FROM clause
          $subquery = $this->getTableOrSubquery($token);
          $from->addTable($subquery);
          break;
        default:
          throw new DBMockParseException("Unexpected {$token['value']}");
      }

      $this->pointer++;
    }

    return tuple($this->pointer, $from);
  }


  private function getTableOrSubquery(token $token): from_table {
    switch ($token['type']) {
      case TokenType::IDENTIFIER:
        return $table = shape('name' => $token['value'], 'join_type' => JoinType::JOIN);
      case TokenType::PAREN:
        // this must be a subquery in the FROM clause
        $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);
        $subquery_tokens = Vec\slice($this->tokens, $this->pointer + 1, $close - $this->pointer - 1);
        if (!C\count($subquery_tokens)) {
          throw new DBMockParseException("Empty parentheses found");
        }
        $this->pointer = $close;
        $expr = new PlaceholderExpression();

        // this will throw if the first keyword isn't SELECT which is what we want
        $subquery_sql = Vec\map($subquery_tokens, $token ==> $token['value']) |> Str\join($$, ' ');
        $parser = new SelectParser(0, $subquery_tokens, $subquery_sql);
        list($p, $select) = $parser->parse();
        $expr = new SubqueryExpression($select, '');

        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
        if ($next !== null && $next['value'] === 'AS') {
          $this->pointer++;
          $next = $this->tokens[$this->pointer];
        }
        if ($next === null || $next['type'] !== TokenType::IDENTIFIER) {
          throw new DBMockParseException("Every subquery must have an alias");
        }
        $name = $next['value'];

        $table = shape(
          'name' => $name,
          'subquery' => $expr,
          'join_type' => JoinType::JOIN,
          'alias' => $name,
        );
        return $table;
      default:
        throw new DBMockParseException("Expected table name or subquery");
    }

  }

  /*
  * Seek as far forward as is needed to build the JOIN expression including join type, conditions, table name and alias
  */
  private function buildJoin(string $left_table, token $token): from_table {

    // INNER JOIN and JOIN are aliases
    $join_type = $token['value'];
    if ($token['value'] === 'INNER')
      $join_type = 'JOIN';

    if (C\contains_key(keyset['INNER', 'CROSS', 'NATURAL'], $token['value'])) {
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next === null || $next['value'] !== 'JOIN') {
        throw new DBMockParseException("Expected keyword JOIN after {$token['value']}");
      }
    } elseif (C\contains_key(keyset['LEFT', 'RIGHT'], $token['value'])) {
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next !== null && $next['value'] === 'OUTER') {
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
      }
      if ($next === null || $next['value'] !== 'JOIN') {
        throw new DBMockParseException("Expected keyword JOIN after {$token['value']}");
      }
    }

    $this->pointer++;
    $next = $this->tokens[$this->pointer] ?? null;
    if ($next === null) {
      throw new DBMockParseException("Expected table or subquery after join keyword");
    }
    $table = $this->getTableOrSubquery($next);
    $table['join_type'] = JoinType::assert($join_type);

    $this->pointer++;
    $next = $this->tokens[$this->pointer] ?? null;
    // it is possible to end the entire query here like "SELECT * from foo join bar"
    if ($next === null) {
      return $table;
    }

    // maybe add alias
    if ($next['type'] === TokenType::IDENTIFIER) {
      $table['alias'] = $next['value'];
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next === null) {
        return $table;
      }
    } elseif ($next['value'] === 'AS') {
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next === null || $next['type'] !== TokenType::IDENTIFIER) {
        throw new DBMockParseException("Expected identifier after AS");
      }
      $table['alias'] = $next['value'];
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next === null) {
        return $table;
      }
    }

    $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);

    // this has to end here if it's a natural or cross join
    // NOTE the natural join filter has to be built later, at runtime
    if (C\contains_key(keyset[JoinType::NATURAL, JoinType::CROSS], $table['join_type'])) {
      return $table;
    }

    // now we need ON or USING
    if (!$next['type'] === TokenType::RESERVED || !C\contains_key(keyset['ON', 'USING'], $next['value'])) {
      throw new DBMockParseException("Expected ON or USING join condition");
    }

    if ($next['value'] === 'USING') {
      $table['join_operator'] = JoinOperator::USING;
      $this->pointer++;
      $next = $this->tokens[$this->pointer] ?? null;
      if ($next === null || $next['type'] !== TokenType::PAREN) {
        throw new DBMockParseException("Expected ( after USING clause");
      }
      $closing_paren_pointer = SQLParser::findMatchingParen($this->pointer, $this->tokens);
      $arg_tokens = Vec\slice($this->tokens, $this->pointer + 1, $closing_paren_pointer - $this->pointer - 1);
      if (!C\count($arg_tokens)) {
        throw new DBMockParseException("Expected at least one argument to USING() clause");
      }
      $count = 0;
      $filter = null;
      foreach ($arg_tokens as $arg) {
        $count++;
        if ($count % 2 === 1) {
          // odd arguments should be columns
          if ($arg['type'] !== TokenType::IDENTIFIER) {
            throw new DBMockParseException("Expected identifier in USING clause");
          }
          $filter = $this->addJoinFilterExpression($filter, $left_table, $table['name'], $arg['value']);
        } elseif ($arg['value'] !== ',') {
          throw new DBMockParseException("Expected , after argument in USING clause");
        }
      }

      $this->pointer = $closing_paren_pointer + 1;
      $table['join_expression'] = $filter;
    } else {
      $table['join_operator'] = JoinOperator::ON;
      $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
      list($this->pointer, $expression) = $expression_parser->buildWithPointer();
      $table['join_expression'] = $expression;
    }

    return $table;
  }

  public function addJoinFilterExpression(
    ?Expression $filter,
    string $left_table,
    string $right_table,
    string $column,
  ): BinaryOperatorExpression {

    $left = new ColumnExpression(shape(
      'type' => TokenType::IDENTIFIER,
      'value' => "{$left_table}.{$column}",
      'raw' => '',
    ));
    $right = new ColumnExpression(shape(
      'type' => TokenType::IDENTIFIER,
      'value' => "{$right_table}.{$column}",
      'raw' => '',
    ));

    // making a binary expression ensuring those two tokens are equal
    $expr = new BinaryOperatorExpression($left, /* $negated */ false, '=', $right);

    // if this is not the first condition, make an AND that wraps the current and new filter
    if ($filter !== null) {
      $filter = new BinaryOperatorExpression($filter, /* $negated */ false, 'AND', $expr);
    } else {
      $filter = $expr;
    }

    return $filter;
  }
}

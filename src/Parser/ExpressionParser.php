<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Str, Vec};

/**
*	The purpose of this class is to build a tree for any expression
*	It doesn't look at actual column values. This is our version of generating a "query plan"
*
*	It uses the list of columns that can exist in the data set to resolve column names in expressions to their indexes,
*	and also resolves constant values to typed data. The goal of this is to generate an expression that can be easily
*	applied to each row of a result set without needing to re-do the parsing logic for every row.
*
*	Expressions might occur in the where clause, join conditions, select clause, having clause, etc.
*	They can often occur inside other expressions, which is why this class acts as a Recursive Descent Parser
*/
final class ExpressionParser {

  /** This represents the precedence of operators. Higher numbers = higher precedence
   * UNARY_MINUS is one we actually rename because it has the same token as subtraction
   * https://dev.mysql.com/doc/refman/5.7/en/operator-precedence.html
   */
  const dict<string, int> OPERATOR_PRECEDENCE = dict[
    'INTERVAL' => 17, // date arithmetic
    'BINARY' => 16, // cast to a binary string
    'COLLATE' => 16, // set collation
    '!' => 15, // NOT
    'UNARY_MINUS' => 14, // unary minus... to keep its name separate from -
    'UNARY_PLUS' => 14, // unary minus... to keep its name separate from -
    '~' => 14, // unary bit inversion
    '^' => 13, // bitwise XOR
    '*' => 12, // multiplication
    '/' => 12, // division
    'DIV' => 12, // integer division
    '%' => 12, // modulus
    'MOD' => 12, // modulus
    '-' => 11, // subtraction
    '+' => 11, // addition
    '<<' => 10, // left bit shift
    '>>' => 10, // right bit shift
    '&' => 9, // bitwise AND
    '|' => 8, // bitwise OR
    '=' => 7, // comparison
    '<=>' => 7, // null-safe equality
    '>=' => 7, // greater than or equal
    '>' => 7, // greater than
    '<=' => 7, // less than or equal
    '<' => 7, // less than
    '<>' => 7, // not equal
    '!=' => 7, // not equal
    'IS' => 7, // boolean equal
    'LIKE' => 7, // string comparison
    'REGEXP' => 7, // regular expression string comparison
    'IN' => 7, // in-list comparison
    'BETWEEN' => 6, // between two values
    'CASE' => 6, // CASE statement
    'WHEN' => 6, // CASE statement
    'THEN' => 6, // CASE statement
    'ELSE' => 6, // CASE statement
    'NOT' => 5, // negation
    'AND' => 4, // boolean AND
    '&&' => 4, // boolean AND
    'XOR' => 3, // boolean XOR
    'OR' => 2, // boolean OR
    '||' => 2, // boolean OR
    'ASSIGNMENT' => 1, // assigning variables
  ];

  private ?vec<Expression> $selectExpressions;


  /**
  * Most of the time, only the first two props are set. List of tokens to iterate on, and column references to index into.
  *
  * The rest are set when we recurse to parse an expression that has higher precedence than the current expression
  * The parent passes tokens and pointer state to the child, which continues until it finds a lower precedence operator,
  * Then returns its state back to the parent (buildWithPointer to move the pointer forward)
  */
  public function __construct(
    private token_list $tokens,
    private int $pointer = -1,
    private Expression $expression = new PlaceholderExpression(),
    public int $min_precedence = 0,
    private bool $is_child = false,
  ) {}

  /** parses an expression that is inside a delimited list, such as function arguments or row expressions
   * i.e.: [col1, col2, col3]
   * return tuple because of a bool indicating whether the "DISTINCT" expression was found
   */
  private function getListExpression(token_list $tokens): (bool, vec<Expression>) {
    $distinct = false;

    $pos = 0;
    $token_count = C\count($tokens);
    $needs_comma = false;

    $args = vec[];

    while ($pos < $token_count) {

      $arg = $tokens[$pos];

      if ($arg['value'] === 'DISTINCT' || $arg['value'] === 'DISTINCTROW') {
        $distinct = true;
        $pos++;
        // DISTINCT can pretend to be a function, unroll it here
        if ($tokens[$pos]['type'] === TokenType::PAREN) {
          $close = SQLParser::findMatchingParen($pos, $tokens);
          $pos++;
          $t = $tokens[$pos];
          if ($close - $pos !== 1) {
            throw new DBMockParseException("Parse error near DISTINCT");
          }
          $p = new ExpressionParser(vec[$t], -1);
          $expr = $p->build();
          $args[] = $expr;
          $pos += 2;
        }
        continue;
      }

      if ($arg['value'] === ',') {
        if ($needs_comma) {
          $needs_comma = false;
          $pos++;
          continue;
        } else {
          throw new DBMockParseException("Unexpected comma in SQL query");
        }
      }
      $p = new ExpressionParser($tokens, $pos - 1);
      list($pos, $expr) = $p->buildWithPointer();
      $args[] = $expr;
      $pos++;
      $needs_comma = true;
    }

    return tuple($distinct, $args);
  }

  // sometimes we identify a token is of a type that it can be immediately turned into an Expression. do that if so
  public function tokenToExpression(token $token): Expression {

    switch ($token['type']) {
      case TokenType::NUMERIC_CONSTANT:
      case TokenType::STRING_CONSTANT:
      case TokenType::NULL_CONSTANT:
        return new ConstantExpression($token);
      case TokenType::IDENTIFIER:
        // if we are processing an expression in the GROUP BY or HAVING or ORDER BY, check the select list first
        // this is because the select may define aliases we can use in these clauses
        // i.e. SELECT something as foo ... GROUP BY foo
        if ($this->selectExpressions is nonnull) {

          foreach ($this->selectExpressions as $expr) {
            if ($expr->name === $token['value']) {
              return $expr;
            }
          }
        }
        return new ColumnExpression($token);
      case TokenType::SQLFUNCTION:
        // function token... just need to resolve the args

        // next token has to be a paren
        $next = $this->nextToken() as nonnull;
        // invariant not exception here because the parser wouldn't have seen it as a function without this
        invariant($next['type'] === TokenType::PAREN, 'function is be followed by parentheses');
        $closing_paren_pointer = SQLParser::findMatchingParen($this->pointer, $this->tokens);

        // process tokens inside the function arguments
        $arg_tokens = Vec\slice($this->tokens, $this->pointer + 1, $closing_paren_pointer - $this->pointer - 1);
        list($distinct, $args) = $this->getListExpression($arg_tokens);

        // move the pointer forward to the end of the parentheses
        $this->pointer = $closing_paren_pointer;

        $fn = new FunctionExpression($token, $args, $distinct);
        return $fn;
      default:
        throw new DBMockNotImplementedException("Not implemented: {$token['value']}");
    }
  }

  /**
   * The main top level API of this class. Builds a nested, evaluatable expression
   */
  public function build(): Expression {
    $token = $this->nextToken();
    $break_while = false;
    while ($token !== null) {
      switch ($token['type']) {
        case TokenType::PAREN:

          $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);
          $arg_tokens = Vec\slice($this->tokens, $this->pointer + 1, $close - $this->pointer - 1);
          if (!C\count($arg_tokens)) {
            throw new DBMockParseException("Empty parentheses found");
          }
          $this->pointer = $close;
          $expr = new PlaceholderExpression();

          if ($arg_tokens[0]['value'] === 'SELECT') {
            $subquery_sql = Vec\map($arg_tokens, $token ==> $token['value']) |> Str\join($$, ' ');
            $parser = new SelectParser(0, $arg_tokens, $subquery_sql);
            list($p, $select) = $parser->parse();
            $expr = new SubqueryExpression($select, '');
          } elseif ($this->expression is InOperatorExpression) {

            $pointer = -1;
            $in_list = vec[];
            $token_count = C\count($arg_tokens);
            while ($pointer < $token_count) {
              $p = new ExpressionParser($arg_tokens, $pointer);
              list($pointer, $expr) = $p->buildWithPointer();
              $in_list[] = $expr;

              if ($pointer + 1 >= $token_count) {
                break;
              }

              $pointer++;
              $next = $arg_tokens[$pointer];
              if ($next['value'] !== ',') {
                throw new DBMockParseException("Expected , in IN () list");
              }
            }
            $this->expression as InOperatorExpression;
            $this->expression->setInList($in_list);
            // we just called set_in_list so skip the setNextChild call
            break;
          } else {
            // in this context, we could either have a sub-expression like
            // (a = b AND c=d) OR a=d
            //
            // or we could have a LIST element, like (a, b, c) < (x, y, z)
            //

            // look for a row expression like (col1, col2, col3)
            $second_token = $arg_tokens[1];
            if ($second_token !== null && $second_token['type'] === TokenType::SEPARATOR) {
              list($distinct, $elements) = $this->getListExpression($arg_tokens);
              if ($distinct) {
                throw new DBMockParseException("Unexpected DISTINCT in row expression");
              }

              $expr = new RowExpression($elements);
            } else {
              $p = new ExpressionParser($arg_tokens, -1);
              $expr = $p->build();
            }
          }


          if ($this->expression is PlaceholderExpression) {
            $this->expression = new BinaryOperatorExpression($expr);
          } else {
            $this->expression->setNextChild($expr);
          }
          break;
        case TokenType::NUMERIC_CONSTANT:
        case TokenType::NULL_CONSTANT:
        case TokenType::STRING_CONSTANT:
        case TokenType::SQLFUNCTION:
        case TokenType::IDENTIFIER:

          // these token types are all not operands and can be parsed on their own into an expression.
          // do that, then let the current expression object figure out where to put it based on its state
          $expr = $this->tokenToExpression($token);

          if ($this->expression is PlaceholderExpression) {
            // we assume an expression will be a Binary Operator (most common) when we first encounter a token
            // if it's something else like BETWEEN or IN, we convert it to that deliberately when encountering the operand
            $this->expression = new BinaryOperatorExpression($expr);
          } elseif (
            !$this->expression->operator &&
            $this->expression is BinaryOperatorExpression &&
            $token['type'] === TokenType::IDENTIFIER
          ) {
            // we encountered an identifier immediately after the left side of an expression... only hope is that this is an implicit alias like
            // "SELECT 1 foo"
            // if so, move the pointer back one and return
            $this->pointer--;
            return $this->expression->left;
          } else {
            $this->expression->setNextChild($expr);
          }
          break;
        case TokenType::OPERATOR:
          // mysql is case insensitive, but php is case sensitive so just uppercase operators for comparisons
          $operator = $token['value'];

          if ($operator === 'CASE') {
            if (!($this->expression is PlaceholderExpression)) {
              // we encountered a CASE statement inside another expression
              // such as "column_name = CASE when ... end"
              // so make a new instance to parse the inner CASE, we'll return back here after the END
              // we move the pointer back by 1 so that the child class encounters the CASE again with a PlaceholderExpression
              $this->pointer = $this->expression
                ->addRecursiveExpression($this->tokens, $this->pointer - 1);
              break;
            }
            $this->expression = new CaseOperatorExpression($token);
            break;
          } elseif (C\contains_key(keyset['WHEN', 'THEN', 'ELSE', 'END'], $operator)) {
            if (!($this->expression is CaseOperatorExpression)) {
              if ($this->expression is BinaryOperatorExpression) {
                // this handles "THEN 1" for example. when we hit token 1 we would have started a binary expression,
                // but we just found the ELSE and we need to just return that constant expression instead
                // also move the pointer back so parent can encounter this keyword
                $this->pointer--;
                return $this->expression->left;
              }
              // it's one of those things where I feel like I should have solved this before...
              // otherwise we just found a keyword outside of a CASE statement, that seems wrong!
              throw new DBMockParseException("Unexpected $operator");
            }
            // tell the case statement we encountered a keyword so it knows where to stuff the next sub-expression
            $this->expression->setKeyword($operator);
            if ($operator !== 'END') {
              // after WHEN, THEN, and ELSE there needs to be a well-formed expression that we have to parse
              $this->pointer = $this->expression
                ->addRecursiveExpression($this->tokens, $this->pointer);
            }

            break;
          }

          // when "EXISTS (foo)"
          // TODO handle EXISTS
          if ($this->expression->operator) {
            if (
              $operator === 'AND' && $this->expression->operator === 'BETWEEN' && !$this->expression->isWellFormed()
            ) {
              $this->expression as BetweenOperatorExpression;
              $this->expression->foundAnd();
            } elseif ($operator === 'NOT') {
              if ($this->expression->operator !== 'IS') {
                $next = $this->peekNext();
                if (
                  $next !== null &&
                  (
                    ($next['type'] === TokenType::OPERATOR && Str\uppercase($next['value']) === 'IN') ||
                    $next['type'] === TokenType::PAREN
                  )
                ) {
                  // something like "A=B AND s NOT IN (foo)", we have to recurse to handle that also
                  // also AND NOT (foo AND bar)
                  $this->pointer = $this->expression
                    ->addRecursiveExpression($this->tokens, $this->pointer, true);
                  break;
                }
                throw new DBMockParseException("Unexpected NOT");
              }
              $this->expression->negate();
            } else {

              // If the new operator has higher precedence, we need to recurse so that it can end up inside the current expression (so it gets evaluated first)
              // Otherwise, we take the entire current expression and nest it inside a new one, which we assume to be Binary for now

              $current_op_precedence = $this->expression->precedence;
              $new_op_precedence = $this->getPrecedence($operator);
              if ($current_op_precedence < $new_op_precedence) {
                // example: 5 + 8 * 3
                // we are at the "*" right now, and have to move "8" out of the "right" from
                // the + operator and into the "left" of the new "*' operator, which gets nested inside the + as its own "right"
                $this->pointer = $this->expression
                  ->addRecursiveExpression($this->tokens, $this->pointer - 1);
              } else {
                // example: 9 / 3 * 3
                // We are at the *. Take the entire current expression, make it be the "left" of a new expression with a "type" of the current operator
                // It's important to nest like this to preserve left-to-right evaluation.
                if ($operator === 'BETWEEN') {
                  $this->expression = new BetweenOperatorExpression($this->expression);
                } elseif ($operator === 'IN') {
                  $this->expression = new InOperatorExpression($this->expression, $this->expression->negated);
                } else {
                  $this->expression = new BinaryOperatorExpression($this->expression, false, $operator);
                }
              }
            }
          } else {
            if ($operator === 'BETWEEN') {
              if (!$this->expression is BinaryOperatorExpression) {
                throw new DBMockParseException('Unexpected keyword BETWEEN');
              }
              $this->expression = new BetweenOperatorExpression($this->expression->left);
            } elseif ($operator === 'NOT') {
              // this negates another operator like "NOT IN" or "IS NOT NULL"
              // operators would throw an DBMockException here if they don't support negation
              $this->expression->negate();
            } elseif ($operator === 'IN') {
              if (!$this->expression is BinaryOperatorExpression) {
                throw new DBMockParseException('Unexpected keyword IN');
              }
              $this->expression = new InOperatorExpression($this->expression->left, $this->expression->negated);
            } elseif ($operator === 'UNARY_MINUS' || $operator === 'UNARY_PLUS' || $operator === '~') {
              $this->expression as PlaceholderExpression;
              $this->expression = new UnaryExpression($operator);
            } else {
              $this->expression as BinaryOperatorExpression;
              $this->expression->setOperator($operator);
            }
          }

          break;
        default:
          throw new DBMockParseException("Expression parse error: unexpected {$token['value']}");
      }

      // don't move pointer forward and break early for some keywords
      $nextToken = $this->peekNext();

      if (!$nextToken) {
        break;
      }

      // return control to parent when we hit one of these
      // special case: VALUES is both a clause and can also be a function inside an INSERT... ON DUPLICATE KEY UPDATE
      // if we find a VALUES and the current expression is incomplete, we keep going
      // TODO actually maybe we should just seek for the VALUES(  sequence in the setParser and collapse those into a sqlFunction token??
      if (C\contains_key(keyset[TokenType::CLAUSE, TokenType::RESERVED, TokenType::SEPARATOR], $nextToken['type'])) {
        if ($nextToken['value'] === 'VALUES' && !$this->expression->isWellFormed()) {
          // change VALUES from a CLAUSE to SQLFUNCTION if it occurs inside an expression
          $this->tokens[$this->pointer + 1]['type'] = TokenType::SQLFUNCTION;
        } else {
          break;
        }
      }

      // possibly break out of the loop depending on next token, operator precedence, child status
      if ($this->expression->isWellFormed()) {

        // alias for the expression?
        if ($nextToken['type'] === TokenType::IDENTIFIER) {
          break;
        }

        // this happens when processing a sub-expression inside of a CASE statement
        // when we encounter the next CASE keyword, and already have a well formed expression, break and let the parent handle it
        if (C\contains_key(keyset['ELSE', 'THEN', 'END'], $nextToken['value'])) {
          break;
        }

        // the only other valid thing to come after a well_formed operation is another operand (other than the things we break on just above)
        if ($nextToken['type'] !== TokenType::OPERATOR) {
          throw new DBMockParseException("Unexpected token {$nextToken['value']}");
        }

        if ($this->is_child) {
          $next_operator_precedence = $this->getPrecedence($nextToken['value']);

          // we are inside a recursive child and found a lower or same precedence operator??
          // then bail, the parent needs to take it from here
          // what matters here is not the current operator's precedence, but the lowest precedence we have seen in this instance
          // (from Precedence Climbing algorithm)
          if ($next_operator_precedence <= $this->min_precedence) {
            break;
          }
        }
      }

      $token = $this->nextToken();
    }

    if (!$this->expression->isWellFormed()) {
      // if we encountered some token like a column, constant, or subquery and we didn't find any more tokens than that, just return that token as the entire expression
      if ($this->expression is BinaryOperatorExpression && $this->expression->operator === '') {
        return $this->expression->left;
      }
      throw new DBMockParseException('Parse error, unexpected end of input');
    }
    return $this->expression;
  }

  public function buildWithPointer(): (int, Expression) {
    $expr = $this->build();
    return tuple($this->pointer, $expr);
  }

  private function nextToken(): ?token {
    $this->pointer++;
    return $this->tokens[$this->pointer] ?? null;
  }

  private function peekNext(): ?token {
    return $this->tokens[$this->pointer + 1] ?? null;
  }

  private function getPrecedence(string $operator): int {
    return self::OPERATOR_PRECEDENCE[$operator] ?? 0;
  }

  /*
  * When parsing expressions in certain places like the GROUP BY or HAVING clauses, it's possible for column references to refer to aliases defined in the SELECT list
  * This function can be called before parsing expressions in those places, so that if a column reference is not found in the tables it can be found from the select list
  */
  public function setSelectExpressions(vec<Expression> $expressions): void {
    $this->selectExpressions = $expressions;
  }
}

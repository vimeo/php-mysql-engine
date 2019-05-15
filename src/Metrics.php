<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Str, Vec};

enum QueryType: string {
  INSERT = 'insert';
  SELECT = 'select';
  UPDATE = 'update';
  DELETE = 'delete';
}

type query_counts = shape(
  QueryType::INSERT => int,
  QueryType::SELECT => int,
  QueryType::UPDATE => int,
  QueryType::DELETE => int,
);

type query_log = shape(
  'type' => QueryType,
  'host' => string,
  'table' => string,
  'sql' => string,
  ?'callstack' => string,
);

/**
 * Store metrics about each query that is executed for tooling, debugging, and introspection
 */
abstract final class Metrics {

  /**
   * Recording call stacks for each query gets expensive,
   * only turn this on if you have a good use for it
   */
  public static bool $enableCallstacks = false;

  /**
   * Filter out function names matching these patterns from the beginning of your callstack to make the stacks more concise
   * uses fnmatch() syntax
   */
  public static keyset<string> $stackIgnorePatterns = keyset[];
  public static vec<query_log> $queryMetrics = vec[];

  public static function getQueryMetrics(): vec<query_log> {
    return self::$queryMetrics;
  }

  public static function reset(): void {
    self::$queryMetrics = vec[];
  }

  public static function getCountByQueryType(): query_counts {
    $totals = shape(
      QueryType::SELECT => 0,
      QueryType::INSERT => 0,
      QueryType::DELETE => 0,
      QueryType::UPDATE => 0,
    );

    foreach (self::$queryMetrics as $metric) {
      switch ($metric['type']) {
        case QueryType::SELECT:
          $totals[QueryType::SELECT]++;
          break;
        case QueryType::INSERT:
          $totals[QueryType::INSERT]++;
          break;
        case QueryType::DELETE:
          $totals[QueryType::DELETE]++;
          break;
        case QueryType::UPDATE:
          $totals[QueryType::UPDATE]++;
          break;
      }
    }

    return $totals;
  }

  public static function getTotalQueryCount(): int {
    return C\count(self::$queryMetrics);
  }

  /**
   * Log a query
   * While a query may hit multiple tables, we only include the first one currently
   */
  public static function trackQuery(QueryType $type, string $host, string $table_name, string $sql): void {
    $metric = shape(
      'type' => $type,
      'host' => $host,
      'table' => $table_name,
      'sql' => $sql,
    );

    if (self::$enableCallstacks) {
      $metric['callstack'] = self::getBacktrace();
    }

    self::$queryMetrics[] = $metric;
  }

  protected static function getBacktrace(): string {
    $trace = \debug_backtrace(\DEBUG_BACKTRACE_IGNORE_ARGS);
    while (!C\is_empty($trace)) {
      $matched = false;

      // filter out this library
      if (Str\contains($trace[0]['file'], __DIR__)) {
        $trace = Vec\drop($trace, 1);
        continue;
      }

      // filter out ignored patterns
      foreach (self::$stackIgnorePatterns as $pattern) {
        if (\fnmatch($pattern, $trace[0]['function'])) {
          $trace = Vec\drop($trace, 1);
          $matched = true;
          break;
        }
      }

      // as soon as we find an item in the trace that isn't in the ignore list, we're done
      if (!$matched) { break; }
    }

    return Vec\reverse($trace)
      |> Vec\map($$, $entry ==> self::formatStackEntry($entry))
      |> Str\join($$, ' -> ');
  }

  /**
   * Returns something like my_file.php:123#my_function()
   */
  protected static function formatStackEntry(
    shape('function' => ?string, 'class' => ?string, ?'file' => string, ?'line' => int) $entry,
  ): string {

    $file = $entry['file'] ?? '';
    $line = $entry['line'] ?? null;
    $function = $entry['function'] ?? '';
    $class = $entry['class'] ?? '';

    $formatted = "";

    # my_file.php
    if (!Str\is_empty($file)) {
      $formatted = Str\split($file, '/') |> $$[C\count($$) - 1];
    }

    # :123
    if ($line is nonnull) {
      $formatted .= ':'.$line;
    }

    # Foo::function() or my_function()
    if (!Str\is_empty($function)) {
      if (!Str\is_empty($class)) {
        $formatted .= '#'.$class.'::'.$function.'()';
      } else {
        $formatted .= '#'.$function.'()';
      }
    }

    return $formatted;
  }

}

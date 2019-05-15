<?hh // strict

namespace Slack\DBMock;

/* HH_FIXME[2049] */
<<__MockClass>>
final class AsyncMysqlConnectResult extends \AsyncMysqlConnectResult {
  private int $elapsed;
  private int $start;

  /* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
  public function __construct(bool $from_pool) {
    // pretend connections take longer if they don't come from the pool
    if ($from_pool) {
      $this->elapsed = 1;
    } else {
      $this->elapsed = 1000;
    }
    $this->start = \time();
  }

  <<__Override>>
  public function elapsedMicros(): int {
    return $this->elapsed;
  }
  <<__Override>>
  public function startTime(): int {
    return $this->start;
  }
  <<__Override>>
  public function endTime(): int {
    return $this->start + $this->elapsed;
  }

  <<__Override>>
  public function clientStats(): \AsyncMysqlClientStats {
    throw new DBMockNotImplementedException('client stats not implemented');
  }
}

<?hh // strict

namespace Slack\DBMock;

// HHAST_IGNORE_ALL[BannedHackCollections]

use namespace HH\Lib\C;

/* HH_FIXME[2049] */
<<__MockClass>>
final class AsyncMysqlQueryResult extends \AsyncMysqlQueryResult {

	/* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
	public function __construct(private vec<dict<string, mixed>> $rows, private int $rows_affected = 0, private int $last_insert_id = 0) {}

	public function numRowsAffected(): int {
		return $this->rows_affected;
	}

	public function lastInsertId(): int {
		return $this->last_insert_id;
	}

	public function numRows(): int {
		return C\count($this->rows);
	}

	public function mapRows(): Vector<Map<string, string>> {
		$out = Vector {};
		foreach ($this->rows as $row) {
			$map = Map {};
			foreach ($row as $column => $value) {
				// in the untyped version, all columns are strings
				$map->set($column, (string)$value);
			}
			$out->add($map);
		}
		return $out;
	}

	public function mapRowsTyped(): Vector<Map<string, mixed>> {
		$out = Vector {};
		foreach ($this->rows as $row) {
			$out->add(new Map($row));
		}
		return $out;
	}

	public function vectorRows(): Vector<Vector<string>> {
		$out = Vector {};
		foreach ($this->rows as $row) {
			$v = Vector {};
			foreach ($row as $value) {
				// in the untyped version, all columns are strings
				$v->add((string)$value);
			}
			$out->add($v);
		}
		return $out;
	}

	public function vectorRowsTyped(): Vector<Vector<mixed>> {
		$out = Vector {};
		foreach ($this->rows as $row) {
			$v = Vector {};
			foreach ($row as $value) {
				$v->add($value);
			}
			$out->add($v);
		}
		return $out;
	}

	public function rowBlocks(): mixed {
		throw new DBMockNotImplementedException('row blocks not implemented');
	}

	public function noIndexUsed(): bool {
		// TODO
		return true;
	}

	public function recvGtid(): string {
		return 'stubbed';
	}
}

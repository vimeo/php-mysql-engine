<?hh // strict

use namespace HH\Lib\Vec;

namespace Slack\SQLFake;

final class SchemaGenerator {

	/**
	 * Pass SQL schema as a string
	 */
	public function generateFromString(string $sql): dict<string, table_schema> {
		$parser = new CreateTableParser();
		$schema = $parser->parse($sql);

		$tables = dict[];
		foreach ($schema as $table => $s) {
			$table_generated_schema = shape(
				'name' => $s['name'],
				'fields' => vec[],
				'indexes' => vec[],
			);

			foreach ($s['fields'] as $field) {
				$f = shape(
					'name' => $field['name'],
					'type' => $field['type'] as DataType,
					'length' => (int)($field['length'] ?? 0),
					'null' => $field['null'] ?? true,
					'hack_type' => $this->sqlToHackFieldType($field),
				);

				$default = ($field['default'] ?? null);
				if ($default is nonnull && $default !== 'NULL') {
					$f['default'] = $default;
				}
				$table_generated_schema['fields'][] = $f;
			}

			foreach ($s['indexes'] as $index) {
				$table_generated_schema['indexes'][] = shape(
					'name' => $index['name'] ?? $index['type'],
					'type' => $index['type'],
					'fields' => Vec\map($index['cols'], $col ==> $col['name']),
				);
			}

			$tables[$table] = $table_generated_schema;
		}

		return $tables;
	}

	/**
	 * Convert a type in SQL to a type in Hack
	 */
	private function sqlToHackFieldType(parsed_field $field): string {
		$name = $field['name'];

		switch ($field['type']) {
			case 'TINYINT':
			case 'SMALLINT':
			case 'INT':
			case 'BIGINT':
				$type = 'int';
				break;

			case 'FLOAT':
			case 'DOUBLE':
				$type = 'float';
				break;

			case 'BINARY':
			case 'CHAR':
			case 'ENUM':
			case 'TINYBLOB':
			case 'BLOB':
			case 'MEDIUMBLOB':
			case 'LONGBLOB':
			case 'TEXT':
			case 'TINYTEXT':
			case 'MEDIUMTEXT':
			case 'LONGTEXT':
			case 'VARCHAR':
			case 'VARBINARY':
			case 'DATE':
			case 'DATETIME':
			// MySQL driver represents these as strings since they are fixed precision
			case 'DECIMAL':
				$type = 'string';
				break;

			default:
				throw new SQLFakeRuntimeException("type {$field['type']} not supported");
				break;
		}

		return $type;
	}
}

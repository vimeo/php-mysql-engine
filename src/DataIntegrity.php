<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Keyset, Vec, Str};

abstract final class DataIntegrity {

  #
  # Ensure that a row has all of the fields specified in the table schema.
  #

  <<__Memoize>>
  public static function namesForSchema(table_schema $schema): keyset<string> {
    return Keyset\map($schema['fields'], $field ==> $field['name']);
  }

  public static function ensureFieldsPresent(dict<string, mixed> $row, table_schema $schema): dict<string, mixed> {

    foreach ($schema['fields'] as $field) {

      $field_name = $field['name'];
      $field_type = $field['hack_type'];
      $field_nullable = $field['null'] ?? false;

      if (!C\contains_key($row, $field_name)) {
        if ($field_nullable) {
          $row[$field_name] = null;
        } else {
          if ($field_type == 'int' || $field_type == 'double') {
            $row[$field_name] = 0;
          } else if ($field_type == 'string') {
            $row[$field_name] = '';
          }
        }
      }
    }

    #
    # TODO(leah): This should really also include a check that all fields that aren't ok being null/unset, e.g. primary key,
    #             were supplied. For now, rely on the opt-in approach + per table review to avoid doing that.
    #

    return $row;
  }

  /**
   * Ensure default values are preesnt, coerce data types as MySQL would
   */
  public static function coerceToSchema(
    dataset $table,
    dict<string, mixed> $row,
    table_schema $schema,
  ): dict<string, mixed> {

    #
    # This call shouldn't be necessary, as ideally we'd always ensure full rows on inserts for anything using typed fetches,
    # but the transition is a little rough, so just do it separately. The extra loop is pretty trivial given this is for tests.
    #

    $fields = self::namesForSchema($schema);
    $bad_fields = Keyset\keys($row) |> Keyset\diff($$, $fields);
    if (!C\is_empty($bad_fields)) {
      $bad_fields = Str\join($bad_fields, ', ');
      throw new DBMockRuntimeException("Column(s) {$bad_fields} not found on {$schema['name']}");
    }
    // TODO put unique key constraint enforcement here

    $row = self::ensureFieldsPresent($row, $schema);

    foreach ($schema['fields'] as $field) {

      $field_name = $field['name'];
      $field_type = $field['hack_type'];

      # don't coerce null values on nullable fields
      if ($field['null'] && $row[$field_name] === null) {
        continue;
      }

      switch ($field_type) {
        case 'int':
          $row[$field_name] = (int)$row[$field_name];
          break;
        case 'string':
          // binary types behave differently than other varchars in MySQL, and we need to mock that behavior
          // specifically, qprintf uses addslashes to escape, and we need to strip those here to match MySQL's behavior
          if (Str\search_ci((string)$field['type'], 'BLOB') !== null) {
            $row[$field_name] = \stripslashes($row[$field_name]);
          } else {
            $row[$field_name] = (string)$row[$field_name];
          }
          break;
        case 'double':
        case 'float':
          $row[$field_name] = (float)$row[$field_name];
          break;
        default:
          throw new DBMockRuntimeException(
            "DataIntegrity::coerceToSchema found unknown type for field: {$field_name}:{$field_type}",
          );
      }
    }

    return $row;
  }

  /**
   * This either returns nothing or throws
   */
  public static function checkUniqueConstraints(dataset $table, dict<string, mixed> $row, table_schema $schema): void {

    // gather all unique keys
    $unique_keys = dict[];
    foreach ($schema['indexes'] as $index) {
      if ($index['type'] === 'PRIMARY') {
        $unique_keys['PRIMARY'] = keyset($index['fields']);
      } elseif ($index['type'] === 'UNIQUE') {
        $unique_keys[$index['name']] = keyset($index['fields']);
      }
    }

    foreach ($unique_keys as $name => $unique_key) {
      // unique key that allows nullable fields? if any of this key's fields on our candidate row are null, skip this key
      // primary keys don't ever allow this
      if ($name !== 'PRIMARY' && C\any($unique_key, $key ==> $row[$key] === null)) {
        continue;
      }

      // are there any existing rows in the table for which every unique key field matches this row?
      if (C\any($table, $r ==> C\every($unique_key, $field ==> $r[$field] === $row[$field]))) {
        $dupe_unique_key_value = Vec\map($unique_key, $field ==> (string)$row[$field]) |> Str\join($$, ', ');
        throw new DBMockUniqueKeyViolation(
          "Duplicate entry '{$dupe_unique_key_value}' for key '{$name}' in table '{$schema['name']}'",
        );
      }
    }
  }
}

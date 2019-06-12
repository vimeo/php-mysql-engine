<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Keyset, Vec, Str};

/**
 * Manages data integrity checks for a table based on its schema
 *
 * Primary and unique keys
 * Ensuring all fields are present with appropriate data types
 * Nullable vs. not nullable
 * Default values
 */
abstract final class DataIntegrity {

  <<__Memoize>>
  public static function namesForSchema(table_schema $schema): keyset<string> {
    return Keyset\map($schema['fields'], $field ==> $field['name']);
  }

  protected static function getDefaultValueForField(
    string $field_type,
    bool $nullable,
    ?string $default,
    string $field_name,
    string $table_name,
  ): mixed {

    if ($default !== null) {
      switch ($field_type) {
        case 'int':
          return Str\to_int($default);
          break;
        case 'double':
          return (float)$default;
          break;
        default:
          return $default;
          break;
      }
    } else if ($nullable) {
      return null;
    }

    if (QueryContext::$strictSQLMode) {
      // if we got this far the column has no default and isn't nullable, strict would throw
      // but default MySQL mode would coerce to a valid value
      throw new SQLFakeRuntimeException("Column '{$field_name}' on '{$table_name}' does not allow null values");
    }

    switch ($field_type) {
      case 'int':
        return 0;
        break;
      case 'double':
        return 0.0;
        break;
      default:
        return '';
        break;
    }
  }

  /**
   * Ensure all fields from the table schema are present in the row
   * Applies default values based on either DEFAULTs, nullable fields, or data types
   */
  public static function ensureFieldsPresent(dict<string, mixed> $row, table_schema $schema): dict<string, mixed> {

    foreach ($schema['fields'] as $field) {

      $field_name = $field['name'];
      $field_type = $field['hack_type'];
      $field_nullable = $field['null'] ?? false;
      $field_default = $field['default'] ?? null;

      if (!C\contains_key($row, $field_name)) {
        $row[$field_name] =
          self::getDefaultValueForField($field_type, $field_nullable, $field_default, $field_name, $schema['name']);
      } else if ($row[$field_name] === null) {
        if ($field_nullable) {
          // explicit null value and nulls are allowed, let it through
          continue;
        } else if (QueryContext::$strictSQLMode) {
          // if we got this far the column has no default and isn't nullable, strict would throw
          // but default MySQL mode would coerce to a valid value
          throw new SQLFakeRuntimeException("Column '{$field_name}' on '{$schema['name']}' does not allow null values");
        } else {
          $row[$field_name] =
            self::getDefaultValueForField($field_type, $field_nullable, $field_default, $field_name, $schema['name']);
        }
      } else {
        // TODO more integrity constraints, check field length for varchars, check timestamps
        switch ($field_type) {
          case 'int':
            if ($row[$field_name] is bool) {
              $row[$field_name] = (int)$row[$field_name];
            } else if (!$row[$field_name] is int) {
              if (QueryContext::$strictSQLMode) {
                $field_str = \var_export($row[$field_name], true);
                throw new SQLFakeRuntimeException(
                  "Invalid value {$field_str} for column '{$field_name}' on '{$schema['name']}', expected int",
                );
              } else {
                $row[$field_name] = (int)$row[$field_name];
              }
            }
            break;
          case 'double':
            if (!$row[$field_name] is float) {
              if (QueryContext::$strictSQLMode) {
                $field_str = \var_export($row[$field_name], true);
                throw new SQLFakeRuntimeException(
                  "Invalid value '{$field_str}' for column '{$field_name}' on '{$schema['name']}', expected float",
                );
              } else {
                $row[$field_name] = (float)$row[$field_name];
              }
            }
            break;
          default:
            if (!$row[$field_name] is string) {
              if (QueryContext::$strictSQLMode) {
                $field_str = \var_export($row[$field_name], true);
                throw new SQLFakeRuntimeException(
                  "Invalid value '{$field_str}' for column '{$field_name}' on '{$schema['name']}', expected string",
                );
              } else {
                $row[$field_name] = (float)$row[$field_name];
              }
            }
            break;
        }
      }
    }

    return $row;
  }

  /**
   * Ensure default values are present, coerce data types as MySQL would
   */
  public static function coerceToSchema(dict<string, mixed> $row, table_schema $schema): dict<string, mixed> {

    $fields = self::namesForSchema($schema);
    $bad_fields = Keyset\keys($row) |> Keyset\diff($$, $fields);
    if (!C\is_empty($bad_fields)) {
      $bad_fields = Str\join($bad_fields, ', ');
      throw new SQLFakeRuntimeException("Column(s) '{$bad_fields}' not found on '{$schema['name']}'");
    }

    $row = self::ensureFieldsPresent($row, $schema);

    foreach ($schema['fields'] as $field) {

      $field_name = $field['name'];
      $field_type = $field['hack_type'];

      // don't coerce null values on nullable fields
      if ($field['null'] && $row[$field_name] === null) {
        continue;
      }

      switch ($field_type) {
        case 'int':
          $row[$field_name] = (int)$row[$field_name];
          break;
        case 'string':
          // binary types behave differently than other varchars in MySQL, and we need to emulate that behavior
          // specifically, qprintf uses addslashes to escape, and we need to strip those here to match MySQL's behavior
          if (Str\search_ci((string)$field['type'], 'BLOB') !== null) {
            $row[$field_name] = \stripslashes((string)$row[$field_name]);
          } else {
            $row[$field_name] = (string)$row[$field_name];
          }
          break;
        case 'double':
        case 'float':
          $row[$field_name] = (float)$row[$field_name];
          break;
        default:
          throw new SQLFakeRuntimeException(
            "DataIntegrity::coerceToSchema found unknown type for field: '{$field_name}:{$field_type}'",
          );
      }
    }

    return $row;
  }

  /**
   * Check for unique key violations
   * If there's a violation, this returns a string message, as well as the integer id of the row that conflicted
   * Caller may decide to throw using the message, or make use of the row id to do an update
   */
  public static function checkUniqueConstraints(
    dataset $table,
    dict<string, mixed> $row,
    table_schema $schema,
    ?int $update_row_id = null,
  ): ?(string, int) {

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
      foreach ($table as $row_id => $r) {
        // if we're updating and this is the row from the original table that we're updating, don't check that one
        if ($row_id === $update_row_id) {
          continue;
        }
        if (C\every($unique_key, $field ==> $r[$field] === $row[$field])) {
          $dupe_unique_key_value = Vec\map($unique_key, $field ==> (string)$row[$field]) |> Str\join($$, ', ');
          return
            tuple("Duplicate entry '{$dupe_unique_key_value}' for key '{$name}' in table '{$schema['name']}'", $row_id);
        }
      }
    }

    return null;
  }
}

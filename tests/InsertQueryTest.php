<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

final class InsertQueryTest extends HackTest {

  private static ?AsyncMysqlConnection $conn;

  <<__Override>>
  public static async function beforeFirstTestAsync(): Awaitable<void> {
    init(TEST_SCHEMA, true);
    $pool = new AsyncMysqlConnectionPool(darray[]);
    static::$conn = await $pool->connect("example", 1, 'db1', '', '');
    // block hole logging
    Logger::setHandle(new \Facebook\CLILib\TestLib\StringOutput());
  }

  <<__Override>>
  public async function beforeEachTestAsync(): Awaitable<void> {
    Server::reset();
    QueryContext::$strictSQLMode = false;
    QueryContext::$strictSchemaMode = false;
  }

  public async function testSingleInsert(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
    $results = await $conn->query("SELECT * FROM table1");
    expect($results->rows())->toBeSame(vec[dict['id' => 1, 'name' => 'test']]);
  }

  public async function testSingleInsertBacktickIdentifiers(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO `table1` (`id`, `name`) VALUES (1, 'test')");
    $results = await $conn->query("SELECT * FROM `table1`");
    expect($results->rows())->toBeSame(vec[dict['id' => 1, 'name' => 'test']]);
  }

  public async function testMultiInsert(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test'), (2, 'test2')");
    $results = await $conn->query("SELECT * FROM table1");
    expect($results->rows())->toBeSame(vec[dict['id' => 1, 'name' => 'test'], dict['id' => 2, 'name' => 'test2']]);
  }

  public async function testPKViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test2')"))->toThrow(
      SQLFakeUniqueKeyViolation::class,
      "Duplicate entry '1' for key 'PRIMARY' in table 'table1'",
    );
  }

  public async function testPKViolationInsertIgnore(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT IGNORE INTO table1 (id, name) VALUES (1, 'test2')"))->notToThrow(
      SQLFakeUniqueKeyViolation::class,
    );
  }

  public async function testPKViolationWithinMultiInsert(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    expect(() ==> $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test'), (1, 'test2')"))->toThrow(
      SQLFakeUniqueKeyViolation::class,
      "Duplicate entry '1' for key 'PRIMARY' in table 'table1'",
    );
  }

  public async function testUniqueViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT INTO table1 (id, name) VALUES (2, 'test')"))->toThrow(
      SQLFakeUniqueKeyViolation::class,
      "Duplicate entry 'test' for key 'name_uniq' in table 'table1'",
    );
  }

  public async function testUniqueViolationInsertIgnore(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT IGNORE INTO table1 (id, name) VALUES (2, 'test')"))->notToThrow(
      SQLFakeUniqueKeyViolation::class,
    );
  }

  public async function testPartialValuesList(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name) VALUES (1, 'test')");
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => null,
      'nullable_default' => 1,
      'not_null_default' => 2,
    ]]);
  }

  public async function testExplicitNullForNullableField(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', null)");
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => null,
      'nullable_default' => 1,
      'not_null_default' => 2,
    ]]);
  }

  public async function testExplicitNullForNotNullableFieldStrict(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    QueryContext::$strictSQLMode = true;
    expect(
      () ==> $conn->query("INSERT INTO table_with_more_fields (id, name, not_null_default) VALUES (1, 'test', null)"),
    )->toThrow(
      SQLFakeRuntimeException::class,
      "Column 'not_null_default' on 'table_with_more_fields' does not allow null values",
    );
  }

  public async function testExplicitNullForNotNullableFieldNotStrict(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name, not_null_default) VALUES (1, 'test', null)");
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => null,
      'nullable_default' => 1,
      'not_null_default' => 2,
    ]]);
  }

  public async function testCompoundPKNoViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT INTO table_with_more_fields (id, name) VALUES (1, 'test2')"))->notToThrow(
      SQLFakeUniqueKeyViolation::class,
    );
  }

  public async function testCompoundPKViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name) VALUES (1, 'test')");
    expect(() ==> $conn->query("INSERT INTO table_with_more_fields (id, name) VALUES (1, 'test')"))->toThrow(
      SQLFakeUniqueKeyViolation::class,
      "Duplicate entry '1, test' for key 'PRIMARY' in table 'table_with_more_fields'",
    );
  }

  public async function testMismatchedValuesList(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    expect(() ==> $conn->query("INSERT INTO table1 (id, name, col3) VALUES (1, 'test2')"))->toThrow(
      SQLFakeParseException::class,
      'Insert list contains 3 fields, but values clause contains 2',
    );
  }

  public async function testNullableUniqueNoViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example')");
    expect(
      () ==> $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (2, 'test2', null)"),
    )
      ->notToThrow(SQLFakeUniqueKeyViolation::class);
  }

  public async function testNullableUniqueViolation(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example')");
    expect(
      () ==>
        $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (2, 'test2', 'example')"),
    )
      ->toThrow(
        SQLFakeUniqueKeyViolation::class,
        "Duplicate entry 'example' for key 'nullable_unique' in table 'table_with_more_fields'",
      );
  }

  public async function testMissingNotNullFieldNoDefault(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table2 (id, table_1_id) VALUES (1, 1)");
    $results = await $conn->query("SELECT * FROM table2");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'table_1_id' => 1,
      'description' => '',
    ]]);
  }

  public async function testMissingNotNullFieldNoDefaultStrict(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    QueryContext::$strictSQLMode = true;
    expect(() ==> $conn->query("INSERT INTO table2 (id, table_1_id) VALUES (1, 1)"))->toThrow(
      SQLFakeRuntimeException::class,
      "Column 'description' on 'table2' does not allow null values",
    );
  }

  public async function testWrongDataType(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table2 (id, table_1_id, description) VALUES (1, 'notastring', 'test')");
    $results = await $conn->query("SELECT * FROM table2");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'table_1_id' => 0,
      'description' => 'test',
    ]]);
  }

  public async function testWrongDataTypeStrict(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    QueryContext::$strictSQLMode = true;
    expect(() ==> $conn->query("INSERT INTO table2 (id, table_1_id, description) VALUES (1, 'notastring', 'test')"))
      ->toThrow(
        SQLFakeRuntimeException::class,
        "Invalid value 'notastring' for column 'table_1_id' on 'table2', expected int",
      );
  }

  public async function testDupeInsertNoConflicts(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query(
      "INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example') ON DUPLICATE KEY UPDATE nullable_default=nullable_default+1",
    );
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => 'example',
      'nullable_default' => 1,
      'not_null_default' => 2,
    ]]);
  }

  public async function testDupeInsertWithConflicts(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query(
      "INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example') ON DUPLICATE KEY UPDATE nullable_default=nullable_default+1",
    );
    await $conn->query(
      "INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example') ON DUPLICATE KEY UPDATE nullable_default=nullable_default+1",
    );
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => 'example',
      'nullable_default' => 2,
      'not_null_default' => 2,
    ]]);
  }

  public async function testDupeInsertWithValuesFunction(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query("INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'example')");
    await $conn->query(
      "INSERT INTO table_with_more_fields (id, name, nullable_unique) VALUES (1, 'test', 'new_example') ON DUPLICATE KEY UPDATE nullable_unique=VALUES(nullable_unique)",
    );
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[dict[
      'id' => 1,
      'name' => 'test',
      'nullable_unique' => 'new_example',
      'nullable_default' => 1,
      'not_null_default' => 2,
    ]]);
  }

  public async function testParseComplexWithEscapedJSONAndComment(): Awaitable<void> {
    $conn = static::$conn as nonnull;
    await $conn->query(
      "INSERT INTO table_with_more_fields (`id`, `name`, `nullable_unique`) VALUES (56789,'{\\\"tes\\'st\\\":\\\"12345\\\"}','test') /* SQL Comment */",
    );
    $results = await $conn->query("SELECT * FROM table_with_more_fields");
    expect($results->rows())->toBeSame(vec[
      dict[
        'id' => 56789,
        'name' => '{"tes\'st":"12345"}',
        'nullable_unique' => 'test',
        'nullable_default' => 1,
        'not_null_default' => 2,
      ],
    ]);
  }
}

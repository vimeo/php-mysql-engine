<?hh // strict

namespace Slack\SQLFake;

abstract class SQLFakeException extends \Exception {}
final class SQLFakeNotImplementedException extends SQLFakeException {}
final class SQLFakeParseException extends SQLFakeException {}
final class SQLFakeRuntimeException extends SQLFakeException {}
final class SQLFakeUniqueKeyViolation extends SQLFakeException {}

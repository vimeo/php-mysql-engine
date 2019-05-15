<?hh // strict

namespace Slack\DBMock;

abstract class DBMockException extends \Exception {}
final class DBMockNotImplementedException extends DBMockException {}
final class DBMockParseException extends DBMockException {}
final class DBMockRuntimeException extends DBMockException {}
final class DBMockUniqueKeyViolation extends DBMockException {}

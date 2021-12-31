<?php
namespace MysqlEngine;

/**
 * Generated enum class, do not extend
 */
final class TokenType
{
    const NUMERIC_CONSTANT = "Number";
    const STRING_CONSTANT = "String";
    const CLAUSE = "Clause";
    const OPERATOR = "Operator";
    const RESERVED = "Reserved";
    const PAREN = "Paren";
    const SEPARATOR = "Separator";
    const SQLFUNCTION = "Function";
    const IDENTIFIER = "Identifier";
    const NULL_CONSTANT = "Null";

    private function __construct()
    {
    }
}

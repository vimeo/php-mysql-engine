<?php

namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\CreateQuery;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\InsertQuery;

final class InsertMultipleParser
{
    /**
     * @return array<string, CreateQuery>
     */
    public function parse(string $sql): array
    {
        return self::walk($this->splitStatements($sql));
    }

    /**
     * @var list<string>
     */
    private $tokens = [];

    /**
     * @var array<int, array{0:int, 1:int}>
     */
    private $sourceMap = [];

    /**
     * @return non-empty-list<string>
     */
    private function splitStatements(string $sql): array
    {
        $re_split_sql = '%
        # Match an SQL record ending with ";"
        \s*                                     # Discard leading whitespace.
        (                                       # $1: Trimmed non-empty SQL record.
          (?:                                   # Group for content alternatives.
            \'[^\'\\\\]*(?:\\\\.[^\'\\\\]*)*\'  # Either a single quoted string,
          | "[^"\\\\]*(?:\\\\.[^"\\\\]*)*"      # or a double quoted string,
          | /\*[^*]*\*+(?:[^*/][^*]*\*+)*/      # or a multi-line comment,
          | \#.*                                # or a # single line comment,
          | --.*                                # or a -- single line comment,
          | [^"\';#]                            # or one non-["\';#-]
          )+                                    # One or more content alternatives
          (?:;|$)                               # Record end is a ; or string end.
        )                                       # End $1: Trimmed SQL record.
        %xs';

        if (preg_match_all($re_split_sql, $sql, $matches)) {
            $statements = $matches[1];
        }

        return $statements ?? [];
    }

    /**
     * @param array<string> $statements
     *
     * @return array<string, InsertQuery>
     */
    private static function walk(array $statements)
    {
        $result = [];

        foreach ($statements as $statement) {
            $statement = trim($statement);
            if (false === stripos($statement, 'INSERT INTO')) {
                continue;
            }
            $statement = rtrim($statement, ';');

            $result[] = SQLParser::parse($statement);
        }

        return $result;
    }
}

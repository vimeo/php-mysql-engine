<?php
namespace MysqlEngine\Tests;

use MysqlEngine\Parser\LexerException;
use MysqlEngine\Parser\ParserException;
use MysqlEngine\Parser\SQLParser;
use MysqlEngine\Query\SelectQuery;
use MysqlEngine\Query\Expression\ColumnExpression;
use MysqlEngine\Query\Expression\CaseOperatorExpression;
use MysqlEngine\Query\Expression\BinaryOperatorExpression;

/**
 * Class SelectParseTest
 * @package MysqlEngine\Tests
 */
class SelectParseTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testSimpleParse(): void
    {
        $query = 'SELECT `foo` FROM `bar` WHERE `id` = 1';

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testParseInvalid(): void
    {
        $query = 'SELECT `foo` FROM `bar` WHERE `id = 1';

        $this->expectException(LexerException::class);

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testParseWithNewline(): void
    {
        $query = 'SELECT `foo`' . "\n" . '.`bar` FROM `bat` WHERE `id` = 1';

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(ColumnExpression::class, $select_query->selectExpressions[0]);

        $this->assertSame('foo', $select_query->selectExpressions[0]->tableName);
        $this->assertSame('bar', $select_query->selectExpressions[0]->columnName);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testAddFunctionResults(): void
    {
        $query = 'SELECT IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)';

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertCount(1, $select_query->selectExpressions);

        $this->assertInstanceOf(
            BinaryOperatorExpression::class,
            $select_query->selectExpressions[0]
        );

        $this->assertTrue($select_query->selectExpressions[0]->isWellFormed());

        $this->assertSame('IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)', $select_query->selectExpressions[0]->name);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testFunctionOperatorPrecedence(): void
    {
        $sql = 'SELECT SUM(`a`.`foo` - IFNULL(`b`.`bar`, 0) - `c`.`baz`) as `a`';

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(
            \MysqlEngine\Query\Expression\FunctionExpression::class,
            $select_query->selectExpressions[0]
        );

        $sum_function = $select_query->selectExpressions[0];

        $this->assertTrue(isset($sum_function->args[0]));

        $this->assertInstanceOf(
            BinaryOperatorExpression::class,
            $sum_function->args[0]
        );

        $this->assertInstanceOf(
            BinaryOperatorExpression::class,
            $sum_function->args[0]->left
        );
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testWrappedSubquery(): void
    {
        $sql = 'SELECT * FROM (((SELECT 5))) AS all_parts';
        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testSimpleCaseCase(): void
    {
        $sql = "SELECT CASE WHEN `a` > 5 THEN 0 ELSE 1 END FROM `bam`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testCaseWhenNotExists(): void
    {
        $sql = "SELECT CASE WHEN NOT EXISTS (SELECT * FROM `bar`) THEN 'BAZ' ELSE NULL END FROM `bam`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testCaseIfPrecedence(): void
    {
        $sql = "SELECT CASE
                    WHEN
                        `a` > 0
                    THEN
                        `e` > 0
                    WHEN
                        TRUE
                    THEN
                        0
                    ELSE
                        1
                END FROM `bam`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(CaseOperatorExpression::class, $select_query->selectExpressions[0]);

        $case = $select_query->selectExpressions[0];

        $this->assertInstanceOf(BinaryOperatorExpression::class, $case->whenExpressions[0]['then']);
        $this->assertSame(118, $case->whenExpressions[0]['then']->start);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testNestedCase(): void
    {
        $sql = "SELECT
                CASE
                    WHEN
                        CASE
                            WHEN
                                `powerups` > 1
                            THEN
                                1
                            ELSE
                                2
                        END > 1
                    THEN
                        3
                    ELSE
                        4
                END
            FROM `video_game_characters`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testCaseThenBoolean(): void
    {
        $sql = "SELECT CASE WHEN `b` NOT IN ('c', 'd') THEN ((`e` < 0) OR `d` NOT LIKE '%bar') ELSE TRUE END FROM `foo`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testInterval(): void
    {
        $sql = 'SELECT DATE_ADD(\'2008-01-02\', INTERVAL 31 DAY)';

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testUnionInSubquery(): void
    {
        $sql = "SELECT *
                FROM  ((SELECT * FROM `c`) UNION ALL (SELECT * FROM `d`)) AS `bar`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testAndNotExists(): void
    {
        $sql = "SELECT *
                FROM `foo`
                WHERE `a` > 0
                AND NOT EXISTS (SELECT * FROM `bar`)";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testSumCaseMultiplied(): void
    {
        $sql = "SELECT SUM((CASE WHEN `a`.`b` THEN 1 ELSE 0 END) * (CASE WHEN `a`.`c` THEN 1 ELSE 0 END))
                 FROM `foo`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testCaseNested(): void
    {
        $sql = "SELECT *
                FROM `foo`
                WHERE CASE
                    WHEN `a` > 0
                    THEN `b` > 0 OR `c` NOT LIKE '%foo'
                    ELSE 0
                END";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(
            \MysqlEngine\Query\Expression\CaseOperatorExpression::class,
            $select_query->whereClause
        );

        $this->assertTrue(
            $select_query->selectExpressions[0]->isWellFormed()
        );
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testBadAs(): void
    {
        $sql = "SELECT (@refund_date := `ordered_transactions`.`refund_date`) AS `r` FROM `foo`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testSumIf(): void
    {
        $query = "SELECT SUM(IF(`a` < 0, 0, 5)) FROM `foo`";

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testParametersHaveCorrectStarts(): void
    {
        $query = "SELECT :foo, :barr, 'hello', 1 FROM `baz`";

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertCount(4, $select_query->selectExpressions);
        $this->assertSame(7, $select_query->selectExpressions[0]->start);
        $this->assertSame(13, $select_query->selectExpressions[1]->start);
        $this->assertSame(20, $select_query->selectExpressions[2]->start);
        $this->assertSame(29, $select_query->selectExpressions[3]->start);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testSelectWithCommentBeforeOffset(): void
    {
        $query = "/* SOME COMMENT */SELECT * FROM `baz`";

        $select_query = SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertSame(18, $select_query->start);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testParseComplexJoin(): void
    {
        $sql = "SELECT * FROM (SELECT * FROM `foo` UNION ALL SELECT * FROM `bar`) AS `baz`";

        $select_query = SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
        $this->assertInstanceOf(\MysqlEngine\Query\FromClause::class, $select_query->fromClause);
        $this->assertCount(1, $select_query->fromClause->tables);
        $this->assertInstanceOf(\MysqlEngine\Query\Expression\SubqueryExpression::class, $select_query->fromClause->tables[0]['subquery']);
        $this->assertNotEmpty($select_query->fromClause->tables[0]['subquery']->query->multiQueries);
        $this->assertSame(15, $select_query->fromClause->tables[0]['subquery']->query->start);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testParseMoreComplex(): void
    {
        $sql = "SELECT `id` FROM `foo`
                JOIN (SELECT @method := 'paypal' AS payout_method) AS d
                JOIN `bar` ON `foo`.`id` = `bar`.`id`";

        SQLParser::parse($sql);
    }

    /**
     * @return void
     * @throws LexerException
     * @throws ParserException
     */
    public function testBracketedFirstSelect(): void
    {
        $sql = "(SELECT * FROM `foo`) UNION ALL (SELECT * FROM `bar`)";

        SQLParser::parse($sql);
    }
}

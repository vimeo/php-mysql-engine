<?php
namespace Vimeo\MysqlEngine\Tests;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;

class SelectParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'SELECT `foo` FROM `bar` WHERE `id` = 1';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testParseInvalid()
    {
        $query = 'SELECT `foo` FROM `bar` WHERE `id = 1';

        $this->expectException(\Vimeo\MysqlEngine\Parser\LexerException::class);

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testParseWithNewline()
    {
        $query = 'SELECT `foo`' . "\n" . '.`bar` FROM `bat` WHERE `id` = 1';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(ColumnExpression::class, $select_query->selectExpressions[0]);

        $this->assertSame('foo', $select_query->selectExpressions[0]->tableName);
        $this->assertSame('bar', $select_query->selectExpressions[0]->columnName);
    }

    public function testAddFunctionResults()
    {
        $query = 'SELECT IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertCount(1, $select_query->selectExpressions);

        $this->assertInstanceOf(
            BinaryOperatorExpression::class,
            $select_query->selectExpressions[0]
        );

        $this->assertTrue($select_query->selectExpressions[0]->isWellFormed());

        $this->assertSame('IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)', $select_query->selectExpressions[0]->name);
    }

    public function testFunctionOperatorPrecedence()
    {
        $sql = 'SELECT SUM(`a`.`foo` - IFNULL(`b`.`bar`, 0) - `c`.`baz`) as `a`';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(
            \Vimeo\MysqlEngine\Query\Expression\FunctionExpression::class,
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

    public function testWrappedSubquery()
    {
        $sql = 'SELECT * FROM (((SELECT 5))) AS all_parts';
        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testSimpleCaseCase()
    {
        $sql = "SELECT CASE WHEN `a` > 5 THEN 0 ELSE 1 END FROM `bam`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testCaseWhenNotExists()
    {
        $sql = "SELECT CASE WHEN NOT EXISTS (SELECT * FROM `bar`) THEN 'BAZ' ELSE NULL END FROM `bam`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testCaseIfPrecedence()
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

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(CaseOperatorExpression::class, $select_query->selectExpressions[0]);

        $case = $select_query->selectExpressions[0];

        $this->assertInstanceOf(BinaryOperatorExpression::class, $case->whenExpressions[0]['then']);
        $this->assertSame(118, $case->whenExpressions[0]['then']->start);
    }

    public function testNestedCase()
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

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testCaseThenBoolean()
    {
        $sql = "SELECT CASE WHEN `b` NOT IN ('c', 'd') THEN ((`e` < 0) OR `d` NOT LIKE '%bar') ELSE TRUE END FROM `foo`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testInterval()
    {
        $sql = 'SELECT DATE_ADD(\'2008-01-02\', INTERVAL 31 DAY)';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testUnionInSubquery()
    {
        $sql = "SELECT *
                FROM  ((SELECT * FROM `c`) UNION ALL (SELECT * FROM `d`)) AS `bar`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testAndNotExists()
    {
        $sql = "SELECT *
                FROM `foo`
                WHERE `a` > 0
                AND NOT EXISTS (SELECT * FROM `bar`)";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testSumCaseMultiplied()
    {
        $sql = "SELECT SUM((CASE WHEN `a`.`b` THEN 1 ELSE 0 END) * (CASE WHEN `a`.`c` THEN 1 ELSE 0 END))
                 FROM `foo`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testCaseNested()
    {
        $sql = "SELECT *
                FROM `foo`
                WHERE CASE
                    WHEN `a` > 0
                    THEN `b` > 0 OR `c` NOT LIKE '%foo'
                    ELSE 0
                END";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertInstanceOf(
            \Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression::class,
            $select_query->whereClause
        );

        $this->assertTrue(
            $select_query->selectExpressions[0]->isWellFormed()
        );
    }

    public function testBadAs()
    {
        $sql = "SELECT (@refund_date := `ordered_transactions`.`refund_date`) AS `r` FROM `foo`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testSumIf()
    {
        $query = "SELECT SUM(IF(`a` < 0, 0, 5)) FROM `foo`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testParametersHaveCorrectStarts()
    {
        $query = "SELECT :foo, :barr, 'hello', 1 FROM `baz`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertCount(4, $select_query->selectExpressions);
        $this->assertSame(7, $select_query->selectExpressions[0]->start);
        $this->assertSame(13, $select_query->selectExpressions[1]->start);
        $this->assertSame(20, $select_query->selectExpressions[2]->start);
        $this->assertSame(29, $select_query->selectExpressions[3]->start);
    }

    public function testSelectWithCommentBeforeOffset()
    {
        $query = "/* SOME COMMENT */SELECT * FROM `baz`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $this->assertSame(18, $select_query->start);
    }

    public function testParseComplexJoin()
    {
        $sql = "SELECT * FROM (SELECT * FROM `foo` UNION ALL SELECT * FROM `bar`) AS `baz`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\FromClause::class, $select_query->fromClause);
        $this->assertCount(1, $select_query->fromClause->tables);
        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\Expression\SubqueryExpression::class, $select_query->fromClause->tables[0]['subquery']);
        $this->assertNotEmpty($select_query->fromClause->tables[0]['subquery']->query->multiQueries);
        $this->assertSame(15, $select_query->fromClause->tables[0]['subquery']->query->start);
    }

    public function testParseMoreComplex()
    {
        $sql = "SELECT `id` FROM `foo`
                JOIN (SELECT @method := 'paypal' AS payout_method) AS d
                JOIN `bar` ON `foo`.`id` = `bar`.`id`";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);
    }

    public function testBracketedFirstSelect()
    {
        $this->markTestSkipped('Broken');
        $sql = "(SELECT * FROM `foo`) UNION ALL (SELECT * FROM `bar`)";

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);
    }

    public function testStrToDateFunction()
    {
        $sql = "SELECT STR_TO_DATE('01,5,2013', '%d,%m,%Y')";
        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($sql);
        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $strToDateFunction = $select_query->selectExpressions[0];
        $this->assertTrue(isset($strToDateFunction->args[0]));
        $this->assertTrue(isset($strToDateFunction->args[1]));
        $this->assertEquals('01,5,2013', $strToDateFunction->args[0]->value);
        $this->assertEquals('%d,%m,%Y', $strToDateFunction->args[1]->value);
    }
}

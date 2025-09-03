<?php

declare(strict_types=1);

namespace Vimeo\MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;
use Vimeo\MysqlEngine\FakePdoInterface;
use Vimeo\MysqlEngine\Processor\Expression\FunctionEvaluator;
use Vimeo\MysqlEngine\Processor\QueryResult;
use Vimeo\MysqlEngine\Processor\Scope;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;

class FunctionEvaluatorTest extends TestCase
{

    public function dataFunction(): array
    {
        return [
            'numeric' => ['SELECT ?', 1],
            ':field'  => ['SELECT :field', ':field'],
            'field'   => ['SELECT :field', 'field'],
        ];
    }

    /**
     * @dataProvider maxValueProvider
     */
    public function testSqlMax(array $rows, ?int $expected) : void
    {
        $conn = $this->createMock(FakePdoInterface::class);
        $scope = $this->createMock(Scope::class);
        $queryResult = $this->createMock(QueryResult::class);
        /** @var array<int, non-empty-array<string, mixed>> $rows */
        $queryResult->rows = $rows;

        $token = new \Vimeo\MysqlEngine\Parser\Token(
            \Vimeo\MysqlEngine\TokenType::SQLFUNCTION,
            'MAX',
            'MAX',
            0
        );

        $exp = new ColumnExpression(
            new \Vimeo\MysqlEngine\Parser\Token(\Vimeo\MysqlEngine\TokenType::IDENTIFIER, "value", '', 0)
        );

        $functionExpr = new FunctionExpression(
            $token,
            [$exp],
            false
        );

        $refMethod = new \ReflectionMethod(FunctionEvaluator::class, 'sqlMax');
        $refMethod->setAccessible(true);

        if ($expected === -1) {
            $this->expectException(\TypeError::class);
            $this->expectExceptionMessage('Bad max value');
        }

        /** @var int|null $actual */
        $actual = $refMethod->invoke(null, $conn, $scope, $functionExpr, $queryResult);

        if ($expected !== -1) {
            $this->assertSame($expected, $actual);
        }
    }

    /**
     * @dataProvider minValueProvider
     */
    public function testSqlMin(array $rows, ?int $expected) : void
    {
        $conn = $this->createMock(FakePdoInterface::class);
        $scope = $this->createMock(Scope::class);
        $queryResult = $this->createMock(QueryResult::class);
        /** @var array<int, non-empty-array<string, mixed>> $rows */
        $queryResult->rows = $rows;

        $token = new \Vimeo\MysqlEngine\Parser\Token(
            \Vimeo\MysqlEngine\TokenType::SQLFUNCTION,
            'MIN',
            'MIN',
            0
        );

        $exp = new ColumnExpression(
            new \Vimeo\MysqlEngine\Parser\Token(\Vimeo\MysqlEngine\TokenType::IDENTIFIER, "value", '', 0)
        );

        $functionExpr = new FunctionExpression(
            $token,
            [$exp],
            false
        );

        $refMethod = new \ReflectionMethod(FunctionEvaluator::class, 'sqlMin');
        $refMethod->setAccessible(true);

        if ($expected === -1) {
            $this->expectException(\TypeError::class);
            $this->expectExceptionMessage('Bad min value');
        }

        /** @var int|null $actual */
        $actual = $refMethod->invoke(null, $conn, $scope, $functionExpr, $queryResult);

        if ($expected !== -1) {
            $this->assertSame($expected, $actual);
        }
    }


    public static function maxValueProvider(): array
    {
        return [
            'null when no rows' => [
                'rows' => [],
                'expected' => null,
            ],
            'max of scalar values' => [
                'rows' => [
                    ['value' => 10],
                    ['value' => 25],
                    ['value' => 5],
                ],
                'expected' => 25,
            ],
            'null values mixed in' => [
                'rows' => [
                    ['value' => null],
                    ['value' => 7],
                    ['value' => null],
                ],
                'expected' => 7,
            ],
            'non scalar values' => [
                'rows' => [
                    ['value' => ['test']],
                ],
                'expected' => -1,
            ],
        ];
    }

    public static function minValueProvider(): array
    {
        return [
            'null when no rows' => [
                'rows' => [],
                'expected' => null,
            ],
            'min of scalar values' => [
                'rows' => [
                    ['value' => 10],
                    ['value' => 25],
                    ['value' => 5],
                ],
                'expected' => 5,
            ],
            'null values mixed in' => [
                'rows' => [
                    ['value' => null],
                    ['value' => 7],
                    ['value' => null],
                ],
                'expected' => null,
            ],
            'non scalar values' => [
                'rows' => [
                    ['value' => ['test']],
                ],
                'expected' => -1,
            ],
        ];
    }
}
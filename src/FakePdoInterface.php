<?php
namespace MysqlEngine;

interface FakePdoInterface
{
    public function getServer(): Server;

    public function setLastInsertId(string $lastInsertId): void;

    public function getDatabaseName(): string;

    public function shouldStringifyResult(): bool;

    public function shouldLowercaseResultKeys(): bool;

    /**
     * @param $name
     * @psalm-suppress MissingParamType
     * @psalm-suppress MissingReturnType
     */
    public function lastInsertId($name = null);

    public function useStrictMode() : bool;
}

<?php
namespace Vimeo\MysqlEngine;

interface FakePdoInterface
{
    public function getServer(): Server;

    public function setLastInsertId(string $last_insert_id): void;

    public function getDatabaseName(): ?string;

    public function shouldStringifyResult(): bool;

    public function shouldLowercaseResultKeys(): bool;

    /**
     * @param  string $seqname
     */
    public function lastInsertId($seqname = null) : string;

    public function useStrictMode() : bool;
}

<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

interface ICsvWriter
{
    public function writeItem(array $item): void;

    public function writeManifest(): void;
}

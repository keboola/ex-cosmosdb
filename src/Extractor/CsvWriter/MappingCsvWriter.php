<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;

class MappingCsvWriter implements ICsvWriter
{
    private string $dataDir;

    private Config $config;

    public function __construct(string $dataDir, Config $config)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
    }

    public function writeItem(array $item): void
    {
    }

    public function writeManifest(): void
    {
    }
}

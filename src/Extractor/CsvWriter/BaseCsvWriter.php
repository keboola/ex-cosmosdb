<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use Keboola\Component\JsonHelper;

abstract class BaseCsvWriter implements ICsvWriter
{
    protected string $dataDir;

    protected Config $config;

    protected array $ignoredKeys;

    public function __construct(string $dataDir, Config $config)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->ignoredKeys = $config->getIgnoredKeys();
    }

    protected function removeIgnoredKeys(object $item): object
    {
        foreach ($this->ignoredKeys as $key) {
            unset($item->$key);
        }

        return $item;
    }

    protected function writeState(object $lastRow): void
    {
        $columnInfo = explode('.', $this->config->getIncrementalFetchingKey());
        JsonHelper::writeFile(
            $this->dataDir . '/out/state.json',
            [
                'lastFetchedRow' => $lastRow->{$columnInfo[1]},
            ]
        );
    }
}

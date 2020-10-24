<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use InvalidArgumentException;
use CosmosDbExtractor\Configuration\Config;
use Keboola\Component\JsonHelper;
use Keboola\Csv\CsvWriter;

class RawCsvWriter implements ICsvWriter
{
    public const ITEM_ID_KEY = 'id';
    public const ID_COLUMN = 'id';
    public const DATA_COLUMN = 'data';

    private Config $config;

    private array $ignoredKeys;

    private string $csvPath;

    private CsvWriter $writer;

    private int $rows = 0;

    public function __construct(string $dataDir, Config $config)
    {
        $this->config = $config;
        $this->ignoredKeys = $config->getIgnoredKeys();
        $this->csvPath = sprintf('%s/out/tables/%s.csv', $dataDir, $config->getOutput());
        $this->writer = new CsvWriter($this->csvPath);
    }

    public function __destruct()
    {
        // No rows -> no CSV file
        if ($this->rows === 0) {
            @unlink($this->csvPath);
        }
    }

    public function writeItem(array $item): void
    {
        // Each Cosmos DB item must have id field
        $id = $item[self::ITEM_ID_KEY] ?? null;
        if (!$id) {
            throw new InvalidArgumentException('Missing item\'s "id" key.');
        }

        // Remove ignored (generated) keys
        foreach ($this->ignoredKeys as $key) {
            unset($item[$key]);
        }

        $this->writer->writeRow([
            self::ID_COLUMN => $id,
            self::DATA_COLUMN => JsonHelper::encode($item),
        ]);

        $this->rows++;
    }

    public function writeManifest(): void
    {
        if ($this->rows > 0) {
            $manifestPath = $this->csvPath . '.manifest';
            file_put_contents($manifestPath, JsonHelper::encode($this->getManifest(), true));
        }
    }

    protected function getManifest(): array
    {
        return [
            'columns' => ['id', 'data'],
            'primary_key' => ['id'],
            'incremental' => $this->config->isIncremental(),
        ];
    }
}

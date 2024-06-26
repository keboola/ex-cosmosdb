<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\ApplicationException;
use CosmosDbExtractor\Exception\UserException;
use Keboola\BigQuery\Extractor\UnloadToCloudStorage\Csv;
use Keboola\BigQuery\Extractor\Utils\IdGenerator;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTable\ManifestOptionsSchema;
use Keboola\Csv\CsvWriter;

class RawCsvWriter extends BaseCsvWriter implements ICsvWriter
{
    public const ITEM_ID_KEY = 'id';
    public const ID_COLUMN = 'id';
    public const DATA_COLUMN = 'data';

    private string $csvPath;

    private CsvWriter $writer;

    private int $rows = 0;

    public function __construct(string $dataDir, Config $config, ManifestManager $manifestManager)
    {
        parent::__construct($dataDir, $config, $manifestManager);
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

    public function writeItem(object $item): void
    {
        $id = $this->getId($item);

        // Remove ignored (generated) keys
        $item = $this->removeIgnoredKeys($item);

        // Write row to CSV
        $this->writer->writeRow([
            self::ID_COLUMN => $id,
            self::DATA_COLUMN => json_encode($item),
        ]);

        $this->lastRow = $item;
        $this->rows++;
    }

    public function finalize(): void
    {
        $this->writeManifest();
    }

    public function writeManifest(): void
    {
        if ($this->rows > 0) {
            $manifestPath = $this->csvPath . '.manifest';
            $options = new ManifestManager\Options\OutTable\ManifestOptions();
            $options
                ->setSchema($this->getSchema())
                ->setIncremental($this->config->isIncremental());

            $this->manifestManager->writeTableManifest(
                $manifestPath,
                $options,
                $this->config->getDataTypeSupport()->usingLegacyManifest(),
            );
        }
    }

    /**
     * @return ManifestOptionsSchema[]
     */
    protected function getSchema(): array
    {
        return [
            new ManifestOptionsSchema(
                'id',
                null,
                false,
                true,
            ),
            new ManifestOptionsSchema(
                'data',
                null,
                true,
                false,
            ),
        ];
    }

    protected function getId(object $item): string
    {
        // Each Cosmos DB item has the ID field
        $id = property_exists($item, self::ITEM_ID_KEY) ? $item->{self::ITEM_ID_KEY} : null;

        if (!$id) {
            if ($this->config->hasSelect()) {
                // ID is missing, because it is not configured in the "select"
                throw new UserException(
                    'Missing "id" key in the query results. ' .
                    'Please modify the "select" value in the configuration ' .
                    'or use the "mapping" mode instead of the "raw".',
                );
            }

            throw new ApplicationException('Missing "id" key in the query results.');
        }

        return (string) $id;
    }
}

<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\ApplicationException;
use CosmosDbExtractor\Exception\UserException;
use Generator;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTable\ManifestOptionsSchema;
use Keboola\CsvMap\Exception\CsvMapperException;
use Keboola\CsvMap\Mapper;
use Keboola\CsvTable\Table;

class MappingCsvWriter extends BaseCsvWriter implements ICsvWriter
{
    private Mapper $mapper;

    public function __construct(string $dataDir, Config $config, ManifestManager $manifestManager)
    {
        parent::__construct($dataDir, $config, $manifestManager);
        try {
            $this->mapper = new Mapper($this->config->getMapping(), false, $this->config->getOutput());
        } catch (CsvMapperException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function writeItem(object $item): void
    {
        // Remove ignored (generated) keys
        $item = $this->removeIgnoredKeys($item);

        // Ensure UNIQUE FK for sub-documents with the SAME CONTENT, but from the DIFFERENT parent document
        $userData = ['parentId' => md5(serialize($item))];
        try {
            $this->mapper->parseRow($item, $userData);
        } catch (CsvMapperException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
        $this->lastRow = $item;
    }

    public function finalize(): void
    {
        $this->copyTempCsvFiles();
        $this->writeManifest();
    }

    protected function copyTempCsvFiles(): void
    {
        foreach ($this->mapper->getCsvFiles() as $csvTable) {
            /** @var Table|null $csvTable */
            if (!$csvTable) {
                // Skip, no row
                continue;
            }

            // Check file size
            $source = $csvTable->getPathName();
            $dest = $this->getCsvTargetPath($csvTable);
            $filesize = filesize($source);
            if ($filesize === false) {
                throw new ApplicationException(sprintf('Failed to get file size "%s".', $source));
            }

            if ($filesize === 0) {
                // No rows -> no CSV file
                continue;
            }

            // Copy
            $result = copy($source, $dest);
            if (!$result) {
                throw new ApplicationException(sprintf('Failed to copy "%s" -> "%s".', $source, $dest));
            }
        }
    }

    protected function writeManifest(): void
    {
        foreach ($this->mapper->getCsvFiles() as $csvTable) {
            /** @var Table|null $csvTable */
            if (!$csvTable) {
                // Skip, no row
                continue;
            }

            // Check if CSV exists
            $csvPath = $this->getCsvTargetPath($csvTable);
            if (!file_exists($csvPath)) {
                // The empty file is not copied, so we also do not create the manifest
                return;
            }

            $options = new ManifestManager\Options\OutTable\ManifestOptions();
            $options
                ->setSchema(iterator_to_array($this->getSchema($csvTable)))
                ->setIncremental($this->config->isIncremental());

            $this->manifestManager->writeTableManifest(
                $csvTable->getName() . '.csv',
                $options,
                $this->config->getDataTypeSupport()->usingLegacyManifest(),
            );
        }
    }

    /**
     * @return Generator<ManifestOptionsSchema>
     */
    protected function getSchema(Table $csvTable): Generator
    {
        /** @var string[]|null $primaryKey */
        $primaryKey = $csvTable->getPrimaryKey(true);
        if ($primaryKey === null) {
            $primaryKey = [];
        }
        foreach ($csvTable->getHeader() as $column) {
            yield new ManifestOptionsSchema(
                $column,
                ['base' => ['type' => 'string']],
                true,
                in_array($column, $primaryKey, true),
            );
        }
    }

    protected function getCsvTargetPath(Table $csvTable): string
    {
        return sprintf('%s/out/tables/%s.csv', $this->dataDir, $csvTable->getName());
    }
}

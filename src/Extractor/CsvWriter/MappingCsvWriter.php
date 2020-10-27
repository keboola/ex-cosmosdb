<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\ApplicationException;
use CosmosDbExtractor\Exception\UserException;
use Keboola\Component\JsonHelper;
use Keboola\CsvMap\Exception\CsvMapperException;
use Keboola\CsvMap\Mapper;
use Keboola\CsvTable\Table;

class MappingCsvWriter extends BaseCsvWriter implements ICsvWriter
{
    private Mapper $mapper;

    public function __construct(string $dataDir, Config $config)
    {
        parent::__construct($dataDir, $config);
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

        // Ensure unique FK for same sub-documents from the different parent document
        $userData = ['parentId' => md5(serialize($item))];
        try {
            $this->mapper->parse([$item], $userData);
        } catch (CsvMapperException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
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
            } elseif ($filesize === 0) {
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

            $manifestPath = $csvPath . '.manifest';
            file_put_contents($manifestPath, JsonHelper::encode($this->getManifest($csvTable), true));
        }
    }

    protected function getManifest(Table $csvTable): array
    {
        return [
            'columns' => $csvTable->getHeader(),
            'primary_key' => $csvTable->getPrimaryKey(true) ?? [],
            'incremental' => $this->config->isIncremental(),
        ];
    }

    protected function getCsvTargetPath(Table $csvTable): string
    {
        return sprintf('%s/out/tables/%s.csv', $this->dataDir, $csvTable->getName());
    }
}

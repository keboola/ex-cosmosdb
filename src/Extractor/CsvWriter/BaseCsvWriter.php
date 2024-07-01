<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\UserException;
use Keboola\Component\JsonHelper;
use Keboola\Component\Manifest\ManifestManager;

abstract class BaseCsvWriter implements ICsvWriter
{
    /**
     * @var string[]
     */
    protected array $ignoredKeys;

    protected ?object $lastRow = null;

    public function __construct(
        protected readonly string $dataDir,
        protected readonly Config $config,
        protected readonly ManifestManager $manifestManager,
    ) {
        $this->ignoredKeys = $config->getIgnoredKeys();
    }

    /**
     * @param array<mixed> $inputState
     */
    public function writeLastState(array $inputState): void
    {
        $lastValue = null;
        if ($this->lastRow) {
            $lastValue = $this->getValueFromRow(
                $this->lastRow,
                $this->config->getIncrementalFetchingKey(),
            );
        } elseif (isset($inputState[Config::STATE_LAST_FETCHED_ROW])) {
            $lastValue = $inputState[Config::STATE_LAST_FETCHED_ROW];
        }

        if ($lastValue) {
            JsonHelper::writeFile(
                $this->dataDir . '/out/state.json',
                [
                    Config::STATE_LAST_FETCHED_ROW => $lastValue,
                ],
            );
        }
    }

    protected function removeIgnoredKeys(object $item): object
    {
        foreach ($this->ignoredKeys as $key) {
            unset($item->$key);
        }

        return $item;
    }

    protected function getValueFromRow(object $lastRow, string $pathString): mixed
    {
        $path = explode('.', $pathString);
        array_shift($path);
        $value = $lastRow;
        foreach ($path as $item) {
            if (isset($value->{$item})) {
                $value = $value->{$item};
            } else {
                throw new UserException(sprintf('Cannot find path "%s".', $pathString));
            }
        }
        if (is_object($value)) {
            throw new UserException(sprintf('Last endpoint of the path "%s" is object.', $pathString));
        }
        return $value;
    }
}

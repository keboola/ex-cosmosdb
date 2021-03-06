<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\UserException;
use Keboola\Component\JsonHelper;

abstract class BaseCsvWriter implements ICsvWriter
{
    protected string $dataDir;

    protected Config $config;

    protected array $ignoredKeys;

    protected ?object $lastRow = null;

    public function __construct(string $dataDir, Config $config)
    {
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->ignoredKeys = $config->getIgnoredKeys();
    }

    public function writeLastState(array $inputState): void
    {
        $lastValue = null;
        if ($this->lastRow) {
            $lastValue = $this->getValueFromRow(
                $this->lastRow,
                $this->config->getIncrementalFetchingKey()
            );
        } elseif (isset($inputState[Config::STATE_LAST_FETCHED_ROW])) {
            $lastValue = $inputState[Config::STATE_LAST_FETCHED_ROW];
        }

        if ($lastValue) {
            JsonHelper::writeFile(
                $this->dataDir . '/out/state.json',
                [
                    Config::STATE_LAST_FETCHED_ROW => $lastValue,
                ]
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

    /**
     * @return mixed
     */
    protected function getValueFromRow(object $lastRow, string $pathString)
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

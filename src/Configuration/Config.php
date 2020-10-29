<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Configuration;

use CosmosDbExtractor\Exception\UndefinedValueException;
use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getEndpoint(): string
    {
        return $this->getValue(['parameters', 'db', 'endpoint']);
    }

    public function getKey(): string
    {
        return $this->getValue(['parameters', 'db', '#key']);
    }

    public function getDatabaseId(): string
    {
        return $this->getValue(['parameters', 'db', 'databaseId']);
    }

    public function getContainerId(): string
    {
        return $this->getValue(['parameters', 'containerId']);
    }

    public function getOutput(): string
    {
        return $this->getValue(['parameters', 'output']);
    }

    public function hasConfigRowId(): bool
    {

        return $this->getValue(['parameters', 'id']) !== null;
    }

    public function getConfigRowId(): int
    {
        if (!$this->hasConfigRowId()) {
            throw new UndefinedValueException('Config row id is not defined.');
        }

        return (int) $this->getValue(['parameters', 'id']);
    }

    public function hasConfigRowName(): bool
    {
        return $this->getValue(['parameters', 'name']) !== null;
    }

    public function getConfigRowName(): string
    {
        if (!$this->hasConfigRowName()) {
            throw new UndefinedValueException('Config row name is not defined.');
        }

        return $this->getValue(['parameters', 'name']);
    }

    public function getMaxTries(): int
    {
        return (int) $this->getValue(['parameters', 'retries']);
    }

    public function getIgnoredKeys(): array
    {
        return $this->getValue(['parameters', 'ignoredKeys']);
    }

    public function hasSelect(): bool
    {
        return $this->getValue(['parameters', 'select']) !== null;
    }

    public function getSelect(): string
    {
        if (!$this->hasSelect()) {
            throw new UndefinedValueException('Select is not defined.');
        }

        return $this->getValue(['parameters', 'select']);
    }

    public function hasFrom(): bool
    {
        return $this->getValue(['parameters', 'from']) !== null;
    }

    public function getFrom(): string
    {
        if (!$this->hasFrom()) {
            throw new UndefinedValueException('From is not defined.');
        }

        return $this->getValue(['parameters', 'from']);
    }

    public function hasSort(): bool
    {
        return $this->getValue(['parameters', 'sort']) !== null;
    }

    public function getSort(): string
    {
        if (!$this->hasSort()) {
            throw new UndefinedValueException('Sort is not defined.');
        }

        return $this->getValue(['parameters', 'sort']);
    }

    public function hasLimit(): bool
    {
        return $this->getValue(['parameters', 'limit']) !== null;
    }

    public function getLimit(): int
    {
        if (!$this->hasLimit()) {
            throw new UndefinedValueException('Limit is not defined.');
        }

        return (int) $this->getValue(['parameters', 'limit']);
    }

    public function hasQuery(): bool
    {
        return $this->getValue(['parameters', 'query']) !== null;
    }

    public function getQuery(): string
    {
        if (!$this->hasQuery()) {
            throw new UndefinedValueException('Query is not defined.');
        }

        return $this->getValue(['parameters', 'query']);
    }

    public function getMode(): string
    {
        return $this->getValue(['parameters', 'mode']);
    }

    public function getMapping(): array
    {
        if ($this->getMode() !== ConfigDefinition::MODE_MAPPING) {
            throw new UndefinedValueException('Mode is not set to mapping.');
        }

        return $this->getValue(['parameters', 'mapping']);
    }

    public function isIncremental(): bool
    {
          return $this->getValue(['parameters', 'incremental']);
    }

    public function hasIncrementalFetchingKey(): bool
    {
        return $this->getValue(['parameters', 'incrementalFetchingKey']) !== null;
    }

    public function getIncrementalFetchingKey(): string
    {
        if (!$this->hasIncrementalFetchingKey()) {
            throw new UndefinedValueException('IncrementalFetchingKey is not defined.');
        }

        return $this->getValue(['parameters', 'incrementalFetchingKey']);
    }
}

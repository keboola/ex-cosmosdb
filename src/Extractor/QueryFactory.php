<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Configuration\Config;

class QueryFactory
{
    private Config $config;

    /**
     * @phpcsSuppress SlevomatCodingStandard.TypeHints.PropertyTypeHint.MissingTraversableTypeHintSpecification
     */
    private array $inputState;

    /**
     * @phpcsSuppress SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingTraversableTypeHintSpecification
     */
    public function __construct(Config $config, array $inputState)
    {
        $this->config = $config;
        $this->inputState = $inputState;
    }

    public function create(): string
    {
        return $this->config->hasQuery() ? $this->config->getQuery() : $this->generate();
    }

    protected function generate(): string
    {
        $sql = [];
        $sql[] = 'SELECT ' . $this->getSelect();
        $sql[] = 'FROM ' . $this->getFrom();

        if ($this->config->hasIncrementalFetchingKey()) {
            if (isset($this->inputState[Config::STATE_LAST_FETCHED_ROW])) {
                $lastFetchedRow = $this->inputState[Config::STATE_LAST_FETCHED_ROW];
                if (is_string($lastFetchedRow)) {
                    $lastFetchedRow = $this->quote($lastFetchedRow);
                }
                $sql[] = sprintf(
                    'WHERE %s >= %s',
                    $this->config->getIncrementalFetchingKey(),
                    $lastFetchedRow,
                );
            }
            $sql[] = 'ORDER BY ' . $this->config->getIncrementalFetchingKey();
        } elseif ($this->config->hasSort()) {
            $sql[] = 'ORDER BY ' . $this->config->getSort();
        }

        if ($this->config->hasLimit()) {
            $sql[] = 'OFFSET 0 LIMIT ' . $this->config->getLimit();
        }

        return implode(' ', $sql);
    }

    protected function getSelect(): string
    {
        return $this->config->hasSelect() ? $this->config->getSelect() : '*';
    }

    protected function getFrom(): string
    {
        return $this->config->hasFrom() ? $this->config->getFrom() : 'c';
    }

    protected function quote(string $str): string
    {
        return sprintf('"%s"', $str);
    }
}

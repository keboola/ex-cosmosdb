<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Configuration\Config;

class QueryFactory
{
    private Config $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function create(): string
    {
        $sql = [];
        $sql[] = 'SELECT ' . $this->getSelect();
        $sql[] = 'FROM ' . $this->getFrom();

        if ($this->config->hasSort()) {
            $sql[] = 'ORDER BY ' . $this->config->getSort();
        }

        if ($this->config->hasLimit()) {
            $sql[] = 'OFFSET 0 LIMIT ' . $this->config->getLimit();
        }

        return implode(' ', $sql);
    }

    private function getSelect(): string
    {
        return $this->config->hasSelect() ? $this->config->getSelect() : '*';
    }

    private function getFrom(): string
    {
        return $this->config->hasFrom() ? $this->config->getFrom() : 'c';
    }
}

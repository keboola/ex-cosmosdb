<?php

declare(strict_types=1);

namespace CosmosDbExtractor;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use CosmosDbExtractor\Extractor\Extractor;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    private Extractor $extractor;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $this->extractor = new Extractor($this->getLogger(), $this->getConfig());
    }

    protected function run(): void
    {
        $this->extractor->extract();
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}

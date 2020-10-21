<?php

declare(strict_types=1);

namespace CosmosDbExtractor;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Configuration\ConfigDefinition;
use Keboola\Component\BaseComponent;
use CosmosDbExtractor\Extractor\Extractor;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    public const ACTION_RUN = 'run';
    public const ACTION_TEST_CONNECTION = 'testConnection';

    private Extractor $extractor;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $this->extractor = new Extractor($this->getLogger(), $this->getConfig());
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_TEST_CONNECTION => 'handleTestConnection'
        ];
    }

    protected function run(): void
    {
        $this->extractor->extract();
    }

    protected function handleTestConnection(): array
    {
        $this->extractor->testConnection();
        return ['success' => true];
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

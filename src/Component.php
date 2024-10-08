<?php

declare(strict_types=1);

namespace CosmosDbExtractor;

use CosmosDbExtractor\Configuration\ActionConfigDefinition;
use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Configuration\ConfigDefinition;
use CosmosDbExtractor\Extractor\Extractor;
use Keboola\Component\BaseComponent;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    public const string ACTION_RUN = 'run';
    public const string ACTION_TEST_CONNECTION = 'testConnection';

    private Extractor $extractor;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $this->extractor = new Extractor(
            $this->getLogger(),
            $this->getDataDir(),
            $this->getConfig(),
            $this->getInputState(),
        );
    }

    /**
     * @return array<string, string>
     */
    protected function getSyncActions(): array
    {
        return [
            self::ACTION_TEST_CONNECTION => 'handleTestConnection',
        ];
    }

    protected function run(): void
    {
        $this->extractor->extract();
    }

    /**
     * @return array{success: bool}
     */
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
        $action = $this->getRawConfig()['action'] ?? Component::ACTION_RUN;
        return $action === Component::ACTION_RUN ? ConfigDefinition::class : ActionConfigDefinition::class;
    }
}

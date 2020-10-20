<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Tests;

use JsonException;
use CosmosDbExtractor\Extractor\JsonDecoder;
use CosmosDbExtractor\Extractor\ProcessFactory;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;
use React\ChildProcess\Process;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;

abstract class AbstractTestCase extends TestCase
{
    protected TestLogger $logger;

    protected LoopInterface $loop;

    protected ProcessFactory $processFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->logger = new TestLogger();
        $this->loop = Factory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    protected function createScriptProcess(string $script): Process
    {
        return $this->processFactory->create(sprintf('node %s/fixtures/%s', __DIR__, $script));
    }
}

<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Configuration\Config;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\Factory as EventLoopFactory;
use React\EventLoop\LoopInterface;

class Extractor
{
    private LoggerInterface $logger;

    private Config $config;

    private LoopInterface  $loop;

    private ProcessFactory $processFactory;

    public function __construct(LoggerInterface $logger, Config $config)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->loop = EventLoopFactory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    public function testConnection(): void
    {
        $process = $this->createNodeJsProcess('testConnection.js');
        $this->loop->run();
    }

    public function extract(): void
    {
        $process = $this->createNodeJsProcess('extract.js');
        $decoder = new JsonDecoder();
        $decoder->processStream($process->stdout, function (array &$document): void {
            $this->writeToCsv($document);
        });

        $this->loop->run();
    }

    protected function writeToCsv(array &$document): void
    {
        // TODO
        var_dump($document);
    }

    protected function createNodeJsProcess(string $script): Process
    {
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s.js', __DIR__, $script));
    }
}

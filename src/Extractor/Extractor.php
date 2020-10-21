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
        // Register a new NodeJs process to event loop.
        // STDOUT output is logged as info message, and STDERR as warning.
        // If the process fails, a ProcessException is thrown.
        // See ProcessFactory class for more info.
        $process = $this->createNodeJsProcess('extract.js');

        // We use separated file descriptor for JSON documents stream.
        $jsonPipe = $process->pipes[ProcessFactory::JSON_STREAM_FD];

        // JSON documents separated by delimiter (see JsonDecoder) are asynchronously read and decoded
        // from the process output (on the separated file descriptor) and converted to CSV.
        $decoder = new JsonDecoder();
        $decoder->processStream($jsonPipe, function (array &$document): void {
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
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s', __DIR__, $script));
    }
}

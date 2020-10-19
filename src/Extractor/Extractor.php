<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Config;
use Psr\Log\LoggerInterface;
use React\EventLoop\Factory as EventLoopFactory;
use React\EventLoop\LoopInterface;
use React\Stream\ReadableStreamInterface;

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

    public function extract(): void
    {
        $process = $this->processFactory->create(sprintf('node %s/NodeJs/extractor.js', __DIR__));
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
}

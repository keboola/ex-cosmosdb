<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Exception\ApplicationException;
use CosmosDbExtractor\Exception\ProcessException;
use CosmosDbExtractor\Exception\UserException;
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
        // Register a new NodeJs process to event loop.
        $process = $this->createNodeJsProcess('testConnection.js', $this->getTestConnectionEnv());

        // On sync actions are logged only errors (no info/warning messages)
        // ... because on sync action success -> JSON output is expected.
        // So we need to capture STDERR and wrap it in an exception on process failure.
        $stderr = '';
        $process->getStderr()->on('data', function (string $chunk) use (&$stderr): void {
            $stderr .= $chunk;
        });

        // Convert process failure to User/Application exception
        $process
            ->getPromise()
            ->done(null, function (\Throwable $e) use (&$stderr): void {
                $msg = trim($stderr ?: $e->getMessage());
                if ($e instanceof ProcessException && $e->getExitCode() === 1) {
                    throw new UserException($msg, $e->getCode(), $e);
                } else {
                    throw new ApplicationException($msg, $e->getCode(), $e);
                }
            });

        // Start event loop
        $this->loop->run();
    }

    public function extract(): void
    {
        // Register a new NodeJs process to event loop.
        // STDOUT output is logged as info message, and STDERR as warning.
        // If the process fails, a ProcessException is thrown.
        // See ProcessFactory for more info.
        $process = $this->createNodeJsProcess('extract.js', $this->getExtractEnv());

        // JSON documents separated by delimiter (see JsonDecoder) are asynchronously read and decoded
        // from the process output (on the separated file descriptor) and converted to CSV.
        $decoder = new JsonDecoder();
        $decoder->processStream($process->getJsonStream(), function (array &$document): void {
            $this->writeToCsv($document);
        });

        // Throw an exception on process failure
        $process
            ->getPromise()
            ->done(null, function (\Throwable $e): void {
                if ($e instanceof ProcessException && $e->getExitCode() === 1) {
                    throw new UserException('Export failed. Check previous messages.', $e->getCode(), $e);
                } else {
                    throw new ApplicationException($e->getMessage(), $e->getCode(), $e);
                }
            });

        // Start event loop
        $this->loop->run();
    }

    protected function writeToCsv(array &$document): void
    {
        // TODO
        var_dump($document);
    }

    protected function getTestConnectionEnv(): array
    {
        return [

        ];
    }

    protected function getExtractEnv(): array
    {
        return array_merge($this->getTestConnectionEnv(), [

        ]);
    }

    protected function createNodeJsProcess(string $script, array $env): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s', __DIR__, $script), $env);
    }
}

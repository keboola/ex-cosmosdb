<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Exception\ProcessException;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use React\Stream\ReadableStreamInterface;

class ProcessFactory
{
    private LoggerInterface $logger;

    private LoopInterface $loop;

    public function __construct(LoggerInterface $logger, LoopInterface $loop)
    {
        $this->logger = $logger;
        $this->loop = $loop;
    }

    public function create(string $cmd): Process
    {
        $process = new Process($cmd);
        $process->start($this->loop);

        // Log process stderr output as warning
        /** @var ReadableStreamInterface $stderr */
        $stderr = $process->stderr;
        $stderr->on('data', function (string $chunk): void {
            $this->logger->warning($chunk);
        });

        // Handle process exit
        $process->on('exit', function (int $exitCode) use ($cmd): void {
            if ($exitCode === 0) {
                $this->logger->info(sprintf('Process "%s" completed successfully.', $cmd));
                return;
            }

            throw new ProcessException(sprintf('Process "%s" exited with code "%d".', $cmd, $exitCode));
        });

        return $process;
    }
}

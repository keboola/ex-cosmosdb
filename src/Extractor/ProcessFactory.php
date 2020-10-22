<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Exception\ProcessException;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;

class ProcessFactory
{
    public const JSON_STREAM_FD = 3;

    private LoggerInterface $logger;

    private LoopInterface $loop;

    public function __construct(LoggerInterface $logger, LoopInterface $loop)
    {
        $this->logger = $logger;
        $this->loop = $loop;
    }

    public function create(string $cmd, array $env = []): ProcessWrapper
    {
        $fileDescriptors = [
            // STDIN
            0 => array('pipe', 'r'),
            // STDOUT
            1 => array('pipe', 'w'),
            // STDERR
            2 => array('pipe', 'w'),
            // JSON STREAM (custom)
            self::JSON_STREAM_FD => array('pipe', 'w'),
        ];

        // Let NodeJs script know which file descriptor should be used to write JSON documents to
        $env['JSON_STREAM_FD'] = self::JSON_STREAM_FD;

        // Create process and attach it to the event loop
        $process = new Process($cmd, null, $env, $fileDescriptors);
        $process->start($this->loop);

        // Log process stdout output as info
        $process->stdout->on('data', function (string $chunk): void {
            $this->logger->info(trim($chunk));
        });

        // Log process stderr output as warning
        $process->stderr->on('data', function (string $chunk): void {
            $this->logger->warning(trim($chunk));
        });

        // Handle process exit
        $deferred = new Deferred();
        $process->on('exit', function (int $exitCode) use ($cmd, $deferred): void {
            if ($exitCode === 0) {
                $this->logger->debug(sprintf('Process "%s" completed successfully.', $cmd));
                $deferred->resolve();
            } else {
                $deferred->reject(
                    new ProcessException(sprintf('Process "%s" exited with code "%d".', $cmd, $exitCode), $exitCode)
                );
            }

            // Make sure the event loop ends
            $this->loop->stop();
        });

        return new ProcessWrapper($process, $deferred->promise());
    }
}

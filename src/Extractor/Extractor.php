<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use CosmosDbExtractor\Extractor\CsvWriter\MappingCsvWriter;
use CosmosDbExtractor\Extractor\CsvWriter\RawCsvWriter;
use UnexpectedValueException;
use CosmosDbExtractor\Configuration\Config;
use CosmosDbExtractor\Configuration\ConfigDefinition;
use CosmosDbExtractor\Exception\ApplicationException;
use CosmosDbExtractor\Exception\ProcessException;
use CosmosDbExtractor\Exception\UserException;
use CosmosDbExtractor\Extractor\CsvWriter\ICsvWriter;
use Psr\Log\LoggerInterface;
use React\EventLoop\Factory as EventLoopFactory;
use React\EventLoop\LoopInterface;

class Extractor
{
    private LoggerInterface $logger;

    private string $dataDir;

    private Config $config;

    private LoopInterface  $loop;

    private ProcessFactory $processFactory;

    private QueryFactory $queryFactory;

    public function __construct(LoggerInterface $logger, string $dataDir, Config $config)
    {
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->loop = EventLoopFactory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
        $this->queryFactory = new QueryFactory($this->config);
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
        $csvWriter = $this->createCsvWriter();

        // Register a new NodeJs process to event loop.
        // STDOUT output is logged as info message, and STDERR as warning.
        // If the process fails, a ProcessException is thrown.
        // See ProcessFactory for more info.
        $process = $this->createNodeJsProcess('extract.js', $this->getExtractEnv());

        // JSON documents separated by delimiter (see JsonDecoder) are asynchronously read and decoded
        // from the process output (on the separated file descriptor) and converted to CSV.
        $decoder = new JsonDecoder();
        $decoder->processStream($process->getJsonStream(), function (array &$item) use ($csvWriter): void {
            $this->writeToCsv($item, $csvWriter);
        });

        // Throw an exception on process failure
        $process
            ->getPromise()
            ->done(null, function (\Throwable $e): void {
                if ($e instanceof ProcessException && $e->getExitCode() === 1) {
                    throw new UserException('Export failed.', $e->getCode(), $e);
                } else {
                    throw new ApplicationException($e->getMessage(), $e->getCode(), $e);
                }
            });

        // Start event loop
        $this->loop->run();

        // Write manifest
        $csvWriter->writeManifest();
    }

    protected function writeToCsv(array &$item, ICsvWriter $csvWriter): void
    {
        $csvWriter->writeItem($item);
    }

    protected function getTestConnectionEnv(): array
    {
        return [
            'JSON_DELIMITER' => json_encode(JsonDecoder::DELIMITER),
            'ENDPOINT' => $this->config->getEndpoint(),
            'KEY' => $this->config->getKey(),
            'DATABASE_ID' => $this->config->getDatabaseId(),
        ];
    }

    protected function getExtractEnv(): array
    {
        return array_merge($this->getTestConnectionEnv(), [
            'CONTAINER_ID' => $this->config->getContainerId(),
            'QUERY' => $this->queryFactory->create(),
        ]);
    }

    protected function createNodeJsProcess(string $script, array $env): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s', __DIR__, $script), $env);
    }

    protected function createCsvWriter(): ICsvWriter
    {
        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_RAW:
                return new RawCsvWriter($this->dataDir, $this->config);
            case ConfigDefinition::MODE_MAPPING:
                return new MappingCsvWriter($this->dataDir, $this->config);
        }

        throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
    }
}
